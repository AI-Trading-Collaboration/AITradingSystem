from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from ai_trading_system import controlled_strategy_batch as controlled
from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_r1_evidence import (
    DEFAULT_R1_ROBUSTNESS_DIR,
    DEFAULT_R1_WALK_FORWARD_DIR,
    validate_r1_robustness_evidence,
    validate_r1_walk_forward_evidence,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_HISTORICAL_SOURCE_ARCHIVE_POLICY_PATH,
    PortableLineageError,
    PortableLineageResolver,
    portable_lineage_failure_evidence,
    require_portable_lineage_archive_sidecar_pair,
)
from ai_trading_system.legacy_research_artifact_portable_lineage import (
    DEFAULT_POLICY_PATH as DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
)
from ai_trading_system.platform.artifacts.writer import (
    write_json_atomic,
    write_markdown_atomic,
)
from ai_trading_system.research_restart import (
    DEFAULT_RESTART_OUTPUT_ROOT,
    DEFAULT_RESTART_POLICY_PATH,
    load_restart_policy,
    validate_research_restart_preflight,
)

SCHEMA_VERSION = "strategy_research_restart_decision.v1"
REPORT_TYPE = "strategy_research_restart_r2_decision"
MANIFEST_TYPE = "strategy_research_restart_r2_manifest"
VALIDATION_TYPE = "strategy_research_restart_r2_validation"
DEFAULT_R0_PREFLIGHT_PATH = DEFAULT_RESTART_OUTPUT_ROOT / "strategy_research_restart_preflight.json"
DEFAULT_FORWARD_MATURITY_PATH = (
    PROJECT_ROOT
    / "outputs"
    / "forward_evidence"
    / "maturity_tracker_r1"
    / "forward_evidence_maturity_tracker.json"
)
DEFAULT_FORWARD_CONTINUITY_PATH = DEFAULT_FORWARD_MATURITY_PATH.with_name(
    "forward_evidence_daily_continuity_maturity_tracker.json"
)
DEFAULT_R2_OUTPUT_ROOT = DEFAULT_RESTART_OUTPUT_ROOT / "r2_decision"

SAFETY = {
    "research_only": True,
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "broker_action": "none",
    "promotion_gate_allowed": False,
    "paper_shadow_change_allowed": False,
    "production_weight_change_allowed": False,
    "shadow_enrollment_allowed": False,
    "automatic_candidate_generation_allowed": False,
    "manual_review_required": True,
}


class ResearchRestartDecisionError(ValueError):
    """Raised when validated R0/R1 evidence cannot produce an R2 decision."""


def run_strategy_research_restart_decision(
    *,
    walk_forward_id: str,
    robustness_id: str,
    r0_preflight_path: Path = DEFAULT_R0_PREFLIGHT_PATH,
    walk_forward_root: Path = DEFAULT_R1_WALK_FORWARD_DIR,
    robustness_root: Path = DEFAULT_R1_ROBUSTNESS_DIR,
    forward_maturity_path: Path = DEFAULT_FORWARD_MATURITY_PATH,
    forward_continuity_path: Path = DEFAULT_FORWARD_CONTINUITY_PATH,
    policy_path: Path = DEFAULT_RESTART_POLICY_PATH,
    output_root: Path = DEFAULT_R2_OUTPUT_ROOT,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    evidence = _validated_evidence(
        walk_forward_id=walk_forward_id,
        robustness_id=robustness_id,
        r0_preflight_path=r0_preflight_path,
        walk_forward_root=walk_forward_root,
        robustness_root=robustness_root,
        forward_maturity_path=forward_maturity_path,
        forward_continuity_path=forward_continuity_path,
    )
    policy = load_restart_policy(policy_path)
    commitments = _input_commitments(
        policy_path=policy_path,
        r0_preflight_path=r0_preflight_path,
        walk_forward_root=walk_forward_root,
        walk_forward_id=walk_forward_id,
        robustness_root=robustness_root,
        robustness_id=robustness_id,
        forward_maturity_path=forward_maturity_path,
        forward_continuity_path=forward_continuity_path,
        evidence=evidence,
    )
    decision_id = (
        "r2-decision_"
        + _stable_id(
            walk_forward_id,
            robustness_id,
            {key: value["sha256"] for key, value in commitments.items()},
            generated.isoformat(),
        )[:16]
    )
    report = _build_decision_report(
        decision_id=decision_id,
        evidence=evidence,
        policy=policy,
        generated=generated,
    )
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / "strategy_research_restart_r2_decision.json"
    markdown_path = report_path.with_suffix(".md")
    manifest_path = output_root / "strategy_research_restart_r2_manifest.json"
    _write_json(report_path, report)
    write_markdown_atomic(markdown_path, render_strategy_research_restart_decision(report))
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": MANIFEST_TYPE,
        "decision_id": decision_id,
        "decision": report["decision"],
        "generated_at": generated.isoformat(),
        "walk_forward_id": walk_forward_id,
        "robustness_id": robustness_id,
        "input_commitments": commitments,
        "output_artifact_checksums": {
            report_path.name: _file_sha256(report_path),
            markdown_path.name: _file_sha256(markdown_path),
        },
        "safety": dict(SAFETY),
    }
    _write_json(manifest_path, manifest)
    return {
        "decision_id": decision_id,
        "decision": report["decision"],
        "status": "PASS",
        "report_path": report_path,
        "manifest_path": manifest_path,
        "report": report,
        "production_effect": "none",
        "broker_action": "none",
    }


def validate_strategy_research_restart_decision(
    *,
    output_root: Path = DEFAULT_R2_OUTPUT_ROOT,
    portable_lineage_sidecar_path: Path | None = None,
    portable_project_root: Path = PROJECT_ROOT,
    portable_lineage_policy_path: Path = DEFAULT_PORTABLE_LINEAGE_POLICY_PATH,
    historical_source_archive_manifest_path: Path | None = None,
    historical_source_archive_policy_path: Path = (DEFAULT_HISTORICAL_SOURCE_ARCHIVE_POLICY_PATH),
) -> dict[str, Any]:
    resolver: PortableLineageResolver | None = None
    require_portable_lineage_archive_sidecar_pair(
        portable_lineage_sidecar_path=portable_lineage_sidecar_path,
        historical_source_archive_manifest_path=historical_source_archive_manifest_path,
    )
    try:
        if portable_lineage_sidecar_path is not None:
            resolver = PortableLineageResolver(
                sidecar_path=portable_lineage_sidecar_path,
                subject_artifact_path=(output_root / "strategy_research_restart_r2_manifest.json"),
                consumer="r2_decision",
                project_root=portable_project_root,
                policy_path=portable_lineage_policy_path,
                historical_source_archive_manifest_path=(historical_source_archive_manifest_path),
                historical_source_archive_policy_path=historical_source_archive_policy_path,
            )
        result = _validate_strategy_research_restart_decision(
            output_root=output_root, resolver=resolver
        )
    except PortableLineageError as exc:
        assert portable_lineage_sidecar_path is not None
        return _portable_r2_validation_failure(
            sidecar_path=portable_lineage_sidecar_path, error=exc
        )
    if resolver is not None:
        result["portable_lineage_resolution"] = resolver.evidence()
    return result


def _validate_strategy_research_restart_decision(
    *, output_root: Path, resolver: PortableLineageResolver | None
) -> dict[str, Any]:
    report_path = output_root / "strategy_research_restart_r2_decision.json"
    markdown_path = report_path.with_suffix(".md")
    manifest_path = output_root / "strategy_research_restart_r2_manifest.json"
    report = _load_json(report_path)
    manifest = _load_json(manifest_path)
    commitments = _mapping(manifest.get("input_commitments"))
    checks = [
        _check("manifest_schema", manifest.get("schema_version") == SCHEMA_VERSION),
        _check("manifest_type", manifest.get("report_type") == MANIFEST_TYPE),
        _check("report_schema", report.get("schema_version") == SCHEMA_VERSION),
        _check("report_type", report.get("report_type") == REPORT_TYPE),
        _check("decision_id_matches", report.get("decision_id") == manifest.get("decision_id")),
        _check("decision_matches", report.get("decision") == manifest.get("decision")),
        _check("safety_boundary", report.get("safety") == SAFETY),
        _check(
            "input_commitments_fresh",
            _commitments_fresh(commitments, resolver=resolver),
        ),
    ]
    output_checksums = _mapping(manifest.get("output_artifact_checksums"))
    for path in (report_path, markdown_path):
        checks.append(
            _check(
                f"output_checksum:{path.name}",
                output_checksums.get(path.name) == _file_sha256(path),
            )
        )
    evidence = _validated_evidence(
        walk_forward_id=str(manifest.get("walk_forward_id", "")),
        robustness_id=str(manifest.get("robustness_id", "")),
        r0_preflight_path=_commitment_path(commitments, "r0_preflight", resolver=resolver),
        walk_forward_root=_commitment_path(
            commitments, "walk_forward_manifest", resolver=resolver
        ).parent.parent,
        robustness_root=_commitment_path(
            commitments, "robustness_manifest", resolver=resolver
        ).parent.parent,
        forward_maturity_path=_commitment_path(commitments, "forward_maturity", resolver=resolver),
        forward_continuity_path=_commitment_path(
            commitments, "forward_continuity", resolver=resolver
        ),
        portable_resolver=resolver,
    )
    policy = load_restart_policy(_commitment_path(commitments, "restart_policy", resolver=resolver))
    recomputed = _build_decision_report(
        decision_id=str(report.get("decision_id", "")),
        evidence=evidence,
        policy=policy,
        generated=datetime.fromisoformat(str(report.get("generated_at"))),
    )
    checks.extend(
        [
            _check("report_content_recomputed", _json_equivalent(report, recomputed)),
            _check(
                "markdown_recomputed",
                markdown_path.read_text(encoding="utf-8")
                == render_strategy_research_restart_decision(recomputed),
            ),
        ]
    )
    passed = all(item["passed"] for item in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_TYPE,
        "decision_id": report.get("decision_id"),
        "decision": report.get("decision"),
        "status": "PASS" if passed else "FAIL",
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
        "broker_action": "none",
    }


def _validated_evidence(
    *,
    walk_forward_id: str,
    robustness_id: str,
    r0_preflight_path: Path,
    walk_forward_root: Path,
    robustness_root: Path,
    forward_maturity_path: Path,
    forward_continuity_path: Path,
    portable_resolver: PortableLineageResolver | None = None,
) -> dict[str, Any]:
    portable_kwargs: dict[str, Any] = {}
    if portable_resolver is not None:
        portable_kwargs = {
            "portable_lineage_sidecar_path": portable_resolver.sidecar_path,
            "portable_project_root": portable_resolver.project_root,
            "portable_lineage_policy_path": portable_resolver.policy_path,
            "historical_source_archive_manifest_path": (
                portable_resolver.historical_source_archive_manifest_path
            ),
            "historical_source_archive_policy_path": (
                portable_resolver.historical_source_archive_policy_path
            ),
        }
    r0_validation = validate_research_restart_preflight(
        artifact_path=r0_preflight_path, **portable_kwargs
    )
    wf_validation = validate_r1_walk_forward_evidence(
        walk_forward_id=walk_forward_id,
        output_dir=walk_forward_root,
        **portable_kwargs,
    )
    robustness_validation = validate_r1_robustness_evidence(
        robustness_id=robustness_id,
        output_dir=robustness_root,
        **portable_kwargs,
    )
    forward_validation = _validate_forward_evidence(
        maturity_path=forward_maturity_path,
        continuity_path=forward_continuity_path,
        resolver=portable_resolver,
    )
    validations = {
        "r0": r0_validation,
        "walk_forward": wf_validation,
        "robustness": robustness_validation,
        "forward": forward_validation,
    }
    failed = [name for name, value in validations.items() if value.get("status") != "PASS"]
    if failed:
        if portable_resolver is not None:
            for name in failed:
                resolution = _mapping(validations[name].get("portable_lineage_resolution"))
                reason_code = str(resolution.get("reason_code", ""))
                if reason_code:
                    raise PortableLineageError(
                        reason_code,
                        f"nested {name} portable-lineage validation failed",
                    )
        raise ResearchRestartDecisionError(
            "R2 requires validated R0/R1 evidence; failed: " + ", ".join(failed)
        )
    return {
        "r0": _load_json(r0_preflight_path),
        "walk_forward": _load_json(
            walk_forward_root / walk_forward_id / "r1_walk_forward_report.json"
        ),
        "robustness": _load_json(robustness_root / robustness_id / "r1_robustness_report.json"),
        "forward_maturity": _load_json(forward_maturity_path),
        "forward_continuity": _load_json(forward_continuity_path),
        "validations": validations,
    }


def _validate_forward_evidence(
    *,
    maturity_path: Path,
    continuity_path: Path,
    resolver: PortableLineageResolver | None = None,
) -> dict[str, Any]:
    maturity = _load_json(maturity_path)
    continuity = _load_json(continuity_path)
    dq = _mapping(maturity.get("data_quality_gate"))
    config_path = _portable_path(Path(str(maturity.get("config_path", ""))), resolver)
    ledger_path = _portable_path(Path(str(maturity.get("ledger_path", ""))), resolver)
    prices_path = _portable_path(Path(str(dq.get("prices_path", ""))), resolver)
    secondary_path = _portable_path(Path(str(dq.get("secondary_prices_path", ""))), resolver)
    rates_path = _portable_path(Path(str(dq.get("rates_path", ""))), resolver)
    config = controlled._load_next_stage_config(config_path)
    universe = controlled._universe(config)
    as_of = date.fromisoformat(str(dq.get("as_of")))
    live_quality = controlled._run_data_quality_gate(
        prices_path=prices_path,
        marketstack_prices_path=secondary_path,
        rates_path=rates_path,
        as_of_date=as_of,
        universe=universe,
    )
    price_rows = controlled._read_price_rows(prices_path, universe=universe)
    dates = controlled._all_dates(price_rows)
    ledger_rows = controlled._read_jsonl_rows(ledger_path)
    maturity_rows = controlled._forward_maturity_rows(
        ledger_rows=ledger_rows, dates=dates, config=config
    )
    maturity_summary = controlled._forward_maturity_summary(maturity_rows)
    daily_continuity = controlled._forward_daily_continuity_report(
        ledger_rows=ledger_rows, dates=dates, config=config
    )
    append_only = controlled._append_only_integrity_report(ledger_rows)
    checks = [
        _check(
            "maturity_report_type",
            maturity.get("report_type") == "forward_evidence_maturity_tracker",
        ),
        _check(
            "continuity_report_type",
            continuity.get("report_type") == "forward_evidence_daily_continuity_maturity_tracker",
        ),
        _check("live_data_quality_pass", live_quality.get("passed") is True),
        _check("data_quality_status_stable", dq.get("status") == live_quality.get("status")),
        _check("ledger_rows_recomputed", maturity.get("ledger_rows") == ledger_rows),
        _check("maturity_rows_recomputed", maturity.get("horizon_maturity") == maturity_rows),
        _check(
            "maturity_summary_recomputed",
            maturity.get("horizon_maturity_summary") == maturity_summary,
        ),
        _check(
            "daily_continuity_recomputed",
            continuity.get("daily_continuity") == daily_continuity,
        ),
        _check(
            "append_only_recomputed",
            continuity.get("append_only_integrity") == append_only,
        ),
        _check(
            "continuity_maturity_matches",
            continuity.get("horizon_maturity") == maturity_summary,
        ),
        _check(
            "forward_safety",
            maturity.get("production_effect") == "none"
            and continuity.get("production_effect") == "none"
            and maturity.get("promotion_gate_allowed") is False
            and continuity.get("promotion_gate_allowed") is False,
        ),
    ]
    passed = all(item["passed"] for item in checks)
    return {
        "status": "PASS" if passed else "FAIL",
        "checks": checks,
        "failed_check_count": sum(1 for item in checks if not item["passed"]),
        "production_effect": "none",
    }


def _build_decision_report(
    *,
    decision_id: str,
    evidence: Mapping[str, Any],
    policy: Mapping[str, Any],
    generated: datetime,
) -> dict[str, Any]:
    r0 = _mapping(evidence.get("r0"))
    wf = _mapping(evidence.get("walk_forward"))
    robustness = _mapping(evidence.get("robustness"))
    maturity = _mapping(evidence.get("forward_maturity"))
    continuity = _mapping(evidence.get("forward_continuity"))
    wf_contract_complete = int(wf.get("evaluation_count", 0)) > 0 and wf.get(
        "evaluation_count"
    ) == wf.get("complete_evaluation_count")
    robustness_contract_complete = robustness.get("evidence_complete") is True
    append_only = _mapping(continuity.get("append_only_integrity")).get("summary", {})
    continuity_summary = _mapping(continuity.get("daily_continuity")).get("summary", {})
    horizon_rows = _records(maturity.get("horizon_maturity_summary"))
    forward_all_mature = bool(horizon_rows) and all(
        int(row.get("pending_count", 0)) == 0
        and int(row.get("matured_count", 0)) == int(row.get("ledger_event_count", -1))
        for row in horizon_rows
    )
    gates = {
        "r0_hard_checks_pass": r0.get("status") == "PASS"
        and r0.get("research_execution_unblocked") is True,
        "walk_forward_contract_complete": wf_contract_complete,
        "walk_forward_negative": wf.get("status") == "FAIL_RESEARCH_EVIDENCE",
        "walk_forward_unbiased_oos_claim_allowed": _mapping(wf.get("oos_summary")).get(
            "unbiased_oos_claim_allowed"
        )
        is True,
        "robustness_contract_complete": robustness_contract_complete,
        "robustness_negative": robustness.get("status") == "FAIL_RESEARCH_EVIDENCE",
        "legacy_only_evidence": wf.get("source_window_role") == "legacy_comparison"
        or robustness.get("source_window_role") == "legacy_comparison",
        "forward_append_only_integrity": append_only.get("append_only_integrity_pass") is True,
        "forward_daily_continuity": continuity_summary.get("daily_continuity_pass") is True,
        "forward_all_horizons_mature": forward_all_mature,
    }
    decision = _select_decision(gates)
    reason_codes = _decision_reason_codes(
        gates=gates, wf=wf, robustness=robustness, continuity=continuity, maturity=maturity
    )
    next_actions = _next_actions(
        gates=gates,
        robustness=robustness,
        continuity=continuity,
        maturity=maturity,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "decision_id": decision_id,
        "status": "PASS",
        "decision": decision,
        "decision_reason_codes": reason_codes,
        "decision_rule_order": _records(_mapping(policy.get("r2_decision")).get("ordered_rules")),
        "gates": gates,
        "evidence_summary": {
            "r0": {
                "status": r0.get("status"),
                "research_execution_unblocked": r0.get("research_execution_unblocked"),
                "failed_check_count": r0.get("failed_check_count"),
            },
            "walk_forward": {
                "status": wf.get("status"),
                "evidence_completeness": wf.get("evidence_completeness"),
                "candidate_count": wf.get("candidate_count"),
                "evaluation_count": wf.get("evaluation_count"),
                "complete_evaluation_count": wf.get("complete_evaluation_count"),
                "oos_summary": wf.get("oos_summary"),
                "source_selection_contamination": wf.get("source_selection_contamination"),
                "locked_holdout_overlap": wf.get("locked_holdout_overlap"),
            },
            "robustness": {
                "status": robustness.get("status"),
                "evidence_complete": robustness.get("evidence_complete"),
                "neighbor_complete": robustness.get("neighbor_complete"),
                "stress_summary": robustness.get("stress_summary"),
                "regime_summary": robustness.get("regime_summary"),
            },
            "forward": {
                "status": maturity.get("status"),
                "ledger_event_count": _mapping(maturity.get("summary")).get("ledger_event_count"),
                "horizon_maturity": horizon_rows,
                "missing_daily_archive_count": continuity_summary.get(
                    "missing_daily_archive_count"
                ),
                "missing_dates": _mapping(continuity.get("daily_continuity")).get(
                    "missing_dates", []
                ),
                "append_only_integrity": append_only.get("append_only_integrity_pass"),
            },
        },
        "window_semantics": _mapping(policy.get("window_semantics")),
        "research_lane": _mapping(policy.get("research_lane")),
        "candidate_expansion_allowed": False,
        "new_parameter_search_allowed": False,
        "simple_selector_status": "KILL",
        "gbdt_status": "PIVOT_DESIGN_ONLY",
        "regret_state_machine_status": "WATCHLIST",
        "next_actions": next_actions,
        "interpretation": (
            "R0 permits research-only execution, but R1 evidence is incomplete and OOS is "
            "negative. Continue evidence closure without new candidate expansion; this is "
            "not a promotion, paper-shadow, production-weight, or broker decision."
        ),
        "generated_at": generated.isoformat(),
        "safety": dict(SAFETY),
        **SAFETY,
    }


def _select_decision(gates: Mapping[str, bool]) -> str:
    if not gates.get("r0_hard_checks_pass"):
        return "HOLD_RESEARCH_RESTART"
    if not gates.get("walk_forward_contract_complete") or not gates.get(
        "robustness_contract_complete"
    ):
        return "CONTINUE_EVIDENCE_CLOSURE"
    if (
        gates.get("walk_forward_negative")
        or gates.get("robustness_negative")
        or gates.get("legacy_only_evidence")
    ):
        return "PAUSE_CANDIDATE_EXPANSION"
    if (
        not gates.get("forward_append_only_integrity")
        or not gates.get("forward_daily_continuity")
        or not gates.get("forward_all_horizons_mature")
    ):
        return "CONTINUE_FORWARD_MATURATION"
    return "READY_FOR_OWNER_CONTROLLED_NEXT_RESEARCH_REVIEW"


def _decision_reason_codes(
    *,
    gates: Mapping[str, bool],
    wf: Mapping[str, Any],
    robustness: Mapping[str, Any],
    continuity: Mapping[str, Any],
    maturity: Mapping[str, Any],
) -> list[str]:
    reasons = []
    if not gates.get("robustness_contract_complete"):
        reasons.append("robustness_regime_sample_incomplete")
    if gates.get("walk_forward_negative"):
        reasons.append("walk_forward_oos_negative")
    if wf.get("source_selection_contamination") is True:
        reasons.append("source_candidate_selection_contaminated")
    if wf.get("locked_holdout_overlap") is True:
        reasons.append("locked_holdout_overlap")
    if gates.get("legacy_only_evidence"):
        reasons.append("legacy_comparison_only")
    missing = _mapping(continuity.get("daily_continuity")).get("missing_dates", [])
    if missing:
        reasons.append("forward_daily_archives_missing")
    pending = [
        str(row.get("horizon"))
        for row in _records(maturity.get("horizon_maturity_summary"))
        if int(row.get("pending_count", 0)) > 0
    ]
    if pending:
        reasons.append("forward_horizons_pending:" + ",".join(pending))
    if robustness.get("source_selection_contamination") is True:
        reasons.append("robustness_source_selection_contaminated")
    return reasons


def _next_actions(
    *,
    gates: Mapping[str, bool],
    robustness: Mapping[str, Any],
    continuity: Mapping[str, Any],
    maturity: Mapping[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not gates.get("robustness_contract_complete"):
        incomplete = [
            {
                "regime": row.get("comparator_id"),
                "row_count": row.get("row_count"),
                "required_row_floor": row.get("required_row_floor"),
            }
            for row in _records(robustness.get("per_regime_comparator"))
            if row.get("evidence_complete") is not True
        ]
        actions.append(
            {
                "action": "continue_regime_evidence_accrual",
                "details": incomplete,
                "constraint": "do_not_lower_sample_floor_to_force_pass",
            }
        )
    missing = _mapping(continuity.get("daily_continuity")).get("missing_dates", [])
    if missing:
        actions.append(
            {
                "action": "owner_review_forward_archive_gap_treatment",
                "missing_dates": list(missing),
                "constraint": "do_not_backfill_or_fabricate_in_this_batch",
            }
        )
    pending = [
        {
            "horizon": row.get("horizon"),
            "matured_count": row.get("matured_count"),
            "pending_count": row.get("pending_count"),
        }
        for row in _records(maturity.get("horizon_maturity_summary"))
        if int(row.get("pending_count", 0)) > 0
    ]
    if pending:
        actions.append(
            {
                "action": "continue_append_only_forward_maturation",
                "pending": pending,
                "constraint": "unified_daily_scheduler_or_runbook_controlled_manual_path_only",
            }
        )
    actions.append(
        {
            "action": "pause_new_candidate_expansion",
            "constraint": "no_new_parameter_search_or_promotion",
        }
    )
    actions.append(
        {
            "action": "design_future_uncontaminated_selection_protocol",
            "constraint": "separate_candidate_selection_from_locked_holdout",
        }
    )
    return actions


def render_strategy_research_restart_decision(payload: Mapping[str, Any]) -> str:
    evidence = _mapping(payload.get("evidence_summary"))
    wf = _mapping(evidence.get("walk_forward"))
    robustness = _mapping(evidence.get("robustness"))
    forward = _mapping(evidence.get("forward"))
    lines = [
        "# 策略研究重启 R2 决策",
        "",
        f"- decision_id: `{payload.get('decision_id')}`",
        f"- decision: `{payload.get('decision')}`",
        "- technical_status: `PASS`",
        "- production_effect: `none`",
        "- broker_action: `none`",
        "",
        "## 真实证据结果",
        "",
        f"- R0: `{_mapping(evidence.get('r0')).get('status')}`；research-only execution 已解锁。",
        (
            f"- Walk-forward: `{wf.get('status')}`；"
            f"{wf.get('complete_evaluation_count')}/{wf.get('evaluation_count')} fold 完整。"
        ),
        (
            f"- Robustness: `{robustness.get('status')}`；"
            f"evidence_complete=`{robustness.get('evidence_complete')}`。"
        ),
        (
            f"- Forward: ledger={forward.get('ledger_event_count')}，"
            f"missing archive={forward.get('missing_daily_archive_count')}。"
        ),
        "",
        "## 为什么不是 READY",
        "",
    ]
    lines.extend(f"- `{reason}`" for reason in _sequence(payload.get("decision_reason_codes")))
    lines.extend(["", "## 后续动作", ""])
    for action in _records(payload.get("next_actions")):
        lines.append(f"- `{action.get('action')}`：{action.get('constraint')}。")
    lines.extend(
        [
            "",
            "## 结论边界",
            "",
            "本决策只说明 R0～R2 证据链当前应如何继续。候选扩展保持暂停；simple selector "
            "保持 KILL，GBDT 保持 PIVOT_DESIGN_ONLY，regret state machine 保持 WATCHLIST。",
            "不得据此启动 promotion、paper-shadow、official/production weight、order "
            "或 broker action。",
            "",
        ]
    )
    return "\n".join(lines)


def _input_commitments(
    *,
    policy_path: Path,
    r0_preflight_path: Path,
    walk_forward_root: Path,
    walk_forward_id: str,
    robustness_root: Path,
    robustness_id: str,
    forward_maturity_path: Path,
    forward_continuity_path: Path,
    evidence: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    wf_dir = walk_forward_root / walk_forward_id
    robustness_dir = robustness_root / robustness_id
    dq = _mapping(_mapping(evidence.get("forward_maturity")).get("data_quality_gate"))
    ledger_path = Path(str(_mapping(evidence.get("forward_maturity")).get("ledger_path", "")))
    paths = {
        "restart_policy": policy_path,
        "r0_preflight": r0_preflight_path,
        "walk_forward_manifest": wf_dir / "r1_wf_manifest.json",
        "walk_forward_report": wf_dir / "r1_walk_forward_report.json",
        "walk_forward_index": wf_dir / "fold_evaluations_index.json",
        "robustness_manifest": robustness_dir / "r1_robustness_manifest.json",
        "robustness_report": robustness_dir / "r1_robustness_report.json",
        "robustness_comparators": robustness_dir / "r1_dedicated_comparators.json",
        "robustness_sensitivity": robustness_dir / "r1_sensitivity.json",
        "forward_maturity": forward_maturity_path,
        "forward_continuity": forward_continuity_path,
        "forward_ledger": ledger_path,
        "prices": Path(str(dq.get("prices_path", ""))),
        "secondary_prices": Path(str(dq.get("secondary_prices_path", ""))),
        "rates": Path(str(dq.get("rates_path", ""))),
    }
    return {name: _commitment(path) for name, path in paths.items()}


def _commitment(path: Path) -> dict[str, Any]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise ResearchRestartDecisionError(f"required R2 input missing: {resolved}")
    return {
        "path": str(resolved),
        "size": resolved.stat().st_size,
        "sha256": _file_sha256(resolved),
    }


def _commitments_fresh(
    commitments: Mapping[str, Any], *, resolver: PortableLineageResolver | None = None
) -> bool:
    if not commitments:
        return False
    for value in commitments.values():
        record = _mapping(value)
        expected_size = _portable_expected_size(record)
        path = _portable_path(
            Path(str(record.get("path", ""))),
            resolver,
            expected_sha256=str(record.get("sha256", "")),
            expected_size=expected_size,
        )
        if (
            not path.is_file()
            or path.stat().st_size != expected_size
            or _file_sha256(path) != record.get("sha256")
        ):
            return False
    return True


def _commitment_path(
    commitments: Mapping[str, Any],
    name: str,
    *,
    resolver: PortableLineageResolver | None = None,
) -> Path:
    record = _mapping(commitments.get(name))
    expected_size = _portable_expected_size(record)
    path = _portable_path(
        Path(str(record.get("path", ""))),
        resolver,
        expected_sha256=str(record.get("sha256", "")),
        expected_size=expected_size,
    )
    if not path.is_file():
        raise ResearchRestartDecisionError(f"R2 commitment missing: {name}")
    return path


def _portable_expected_size(record: Mapping[str, Any]) -> int:
    try:
        value = int(record.get("size", -1))
    except (TypeError, ValueError) as exc:
        raise PortableLineageError(
            "SOURCE_EXPECTATION_MISMATCH", "commitment size is invalid"
        ) from exc
    if value < 0:
        raise PortableLineageError("SOURCE_EXPECTATION_MISMATCH", "commitment size is invalid")
    return value


def _portable_path(
    path: Path,
    resolver: PortableLineageResolver | None,
    *,
    expected_sha256: str | None = None,
    expected_size: int | None = None,
) -> Path:
    if resolver is None:
        return path
    return resolver.resolve(
        path,
        expected_sha256=expected_sha256,
        expected_size=expected_size,
    )


def _portable_r2_validation_failure(
    *, sidecar_path: Path, error: PortableLineageError
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_TYPE,
        "decision_id": None,
        "decision": None,
        "status": "FAIL",
        "checks": [
            {
                "check_id": "portable_lineage_resolution",
                "passed": False,
                "reason_code": error.reason_code,
            }
        ],
        "failed_check_count": 1,
        "portable_lineage_resolution": portable_lineage_failure_evidence(
            error=error,
            consumer="r2_decision",
            sidecar_path=sidecar_path,
        ),
        "production_effect": "none",
        "broker_action": "none",
    }


def _stable_id(*values: Any) -> str:
    encoded = json.dumps(values, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload, default=str)


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ResearchRestartDecisionError(f"required JSON missing: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ResearchRestartDecisionError(f"required JSON is not a mapping: {path}")
    return dict(value)


def _check(check_id: str, passed: bool) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed)}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return list(value)


def _records(value: Any) -> list[dict[str, Any]]:
    return [_mapping(item) for item in _sequence(value)]


def _json_equivalent(left: Any, right: Any) -> bool:
    return json.dumps(left, sort_keys=True, default=str) == json.dumps(
        right, sort_keys=True, default=str
    )
