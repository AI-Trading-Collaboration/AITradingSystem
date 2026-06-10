from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_backtest_simulation import (
    DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_historical_replay import (
    _artifact_dir_from_latest,
    _check,
    _float,
    _int,
    _mapping,
    _read_json,
    _read_jsonl,
    _read_optional_json,
    _records,
    _stable_id,
    _text,
    _unique_dir,
    _update_latest_pointer,
    _validation_payload,
    _write_json,
    _write_jsonl,
    _write_text,
)
from ai_trading_system.etf_portfolio.dynamic_v3_outcome_accumulation import (
    DEFAULT_CONSENSUS_RISK_DIR,
    DEFAULT_LIMITED_VS_NOTRADE_DIR,
)
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    SCHEMA_VERSION,
)

DEFAULT_CONFIRMATION_REGISTRY_DIR = (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "forward_confirmation_registry"
)
DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH = (
    PROJECT_ROOT
    / "registry"
    / "etf_portfolio"
    / "dynamic_v3_rescue_forward_confirmation_targets.yaml"
)
DEFAULT_CONFIRMATION_PROGRESS_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_progress"
DEFAULT_CONFIRMATION_EVALUATION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "confirmation_evaluation"
DEFAULT_RULE_REVIEW_CYCLE_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_review_cycle"
DEFAULT_RULE_OWNER_DECISION_DIR = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT / "rule_owner_decision"
DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH = (
    DEFAULT_RULE_OWNER_DECISION_DIR / "rule_owner_decision_journal.jsonl"
)

PROGRESS_STATUSES = {
    "INSUFFICIENT_EVENTS",
    "IN_PROGRESS",
    "NEAR_READY",
    "READY_FOR_EVALUATION",
    "BLOCKED",
}
EVALUATION_STATUSES = {"NOT_READY", "SUCCESS", "FAILURE", "MIXED", "REVIEW_REQUIRED"}
RULE_REVIEW_DECISIONS = {
    "CONTINUE_TRACKING",
    "READY_FOR_OWNER_REVIEW",
    "KEEP_CURRENT_RULES",
    "TIGHTEN_RULES_RECOMMENDED",
    "LOOSEN_RULES_RECOMMENDED",
    "KEEP_REFERENCE_ONLY",
    "RENAME_OR_RECLASSIFY",
    "DEFER",
}
OWNER_DECISIONS = {
    "pending",
    "continue_tracking",
    "keep_current_rules",
    "request_more_data",
    "approve_manual_policy_review",
    "reject_rule_change",
    "defer",
}


class DynamicV3ConfirmationCycleError(ValueError):
    """Raised when forward confirmation cycle inputs or artifacts are invalid."""


def register_confirmation_targets(
    *,
    confirmation_plan_id: str,
    confirmation_plan_dir: Path = DEFAULT_FORWARD_CONFIRMATION_PLAN_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    registry_yaml_path: Path = DEFAULT_CONFIRMATION_REGISTRY_YAML_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    plan_dir = confirmation_plan_dir / confirmation_plan_id
    plan_manifest = _read_json(plan_dir / "confirmation_plan_manifest.json")
    plan_targets = _read_json(plan_dir / "confirmation_targets.json")
    plan_failures = _read_optional_json(plan_dir / "failure_conditions.json") or {}
    targets = [
        _registry_target(row, plan_failures)
        for row in _records(plan_targets.get("targets"))
    ]
    registry_id = _stable_id("forward-confirmation-registry", confirmation_plan_id, generated)
    registry_dir = _unique_dir(output_dir / registry_id)
    registry_dir.mkdir(parents=True, exist_ok=False)
    active_count = sum(1 for row in targets if row["status"] == "active")
    watch_only_count = sum(1 for row in targets if row["status"] == "watch_only")
    registry = {
        "schema_version": SCHEMA_VERSION,
        "source_confirmation_plan_id": confirmation_plan_id,
        "created_at": generated.isoformat(),
        "status": "active",
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_confirmation_registry_manifest",
        "registry_id": registry_dir.name,
        "source_confirmation_plan_id": confirmation_plan_id,
        "source_confirmation_plan_status": _text(plan_manifest.get("status")),
        "generated_at": generated.isoformat(),
        "status": "PASS" if targets else "FAIL",
        "targets_total": len(targets),
        "active_target_count": active_count,
        "watch_only_target_count": watch_only_count,
        "confirmation_registry_manifest_path": str(
            registry_dir / "confirmation_registry_manifest.json"
        ),
        "registered_targets_path": str(registry_dir / "registered_targets.yaml"),
        "confirmation_targets_report_path": str(registry_dir / "confirmation_targets_report.md"),
        "registry_yaml_path": str(registry_yaml_path),
        "market_regime": "ai_after_chatgpt",
        "auto_apply": False,
        "owner_approval_required": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(registry_dir / "confirmation_registry_manifest.json", manifest)
    _write_yaml(registry_dir / "registered_targets.yaml", registry)
    _write_text(
        registry_dir / "confirmation_targets_report.md",
        render_confirmation_registry_report(manifest, registry),
    )
    _write_yaml(registry_yaml_path, {**registry, "registry_id": registry_dir.name})
    _update_latest_pointer(
        "latest_forward_confirmation_registry",
        registry_dir.name,
        registry_dir / "confirmation_registry_manifest.json",
    )
    return {
        "registry_id": registry_dir.name,
        "registry_dir": registry_dir,
        "manifest": manifest,
        "registry": registry,
        "targets": targets,
    }


def confirmation_targets_report_payload(
    *,
    registry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    registry_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=registry_id if not latest else None,
        pointer_name="latest_forward_confirmation_registry",
    )
    return {
        **_read_json(registry_dir / "confirmation_registry_manifest.json"),
        "registered_targets": _read_yaml(registry_dir / "registered_targets.yaml"),
        "confirmation_targets_report": _read_text(registry_dir / "confirmation_targets_report.md"),
        "registry_dir": str(registry_dir),
    }


def list_confirmation_targets(
    *,
    registry_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    payload = confirmation_targets_report_payload(
        registry_id=registry_id,
        latest=latest or registry_id is None,
        output_dir=output_dir,
    )
    registry = _mapping(payload.get("registered_targets"))
    targets = _records(registry.get("targets"))
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_forward_confirmation_target_list",
        "registry_id": payload.get("registry_id"),
        "targets_total": len(targets),
        "active_target_count": sum(1 for row in targets if row.get("status") == "active"),
        "watch_only_target_count": sum(1 for row in targets if row.get("status") == "watch_only"),
        "targets": targets,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
        "owner_approval_required": True,
    }


def validate_confirmation_targets_artifact(
    *,
    registry_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
) -> dict[str, Any]:
    registry_dir = output_dir / registry_id
    manifest = _read_optional_json(registry_dir / "confirmation_registry_manifest.json") or {}
    registry = _read_yaml_optional(registry_dir / "registered_targets.yaml")
    targets = _records(registry.get("targets"))
    target_ids = [_text(row.get("target_id")) for row in targets]
    checks = [
        _check(
            "manifest_exists",
            (registry_dir / "confirmation_registry_manifest.json").exists(),
            "",
        ),
        _check("targets_yaml_exists", (registry_dir / "registered_targets.yaml").exists(), ""),
        _check("report_exists", (registry_dir / "confirmation_targets_report.md").exists(), ""),
        _check("registry_id_matches", manifest.get("registry_id") == registry_id, ""),
        _check("target_ids_unique", len(target_ids) == len(set(target_ids)), "target_id"),
        _check("target_count_nonzero", len(targets) > 0, "targets"),
        _check(
            "required_targets_present",
            set(target_ids)
            >= {
                "limited_adjustment_vs_no_trade",
                "defensive_limited_adjustment_drawdown",
                "consensus_target_risk",
            },
            "targets",
        ),
        _check(
            "auto_apply_forbidden",
            manifest.get("auto_apply") is False
            and all(row.get("auto_apply") is False for row in targets),
            "auto_apply=false",
        ),
        _check(
            "owner_approval_required",
            manifest.get("owner_approval_required") is True
            and all(row.get("owner_approval_required") is True for row in targets),
            "owner approval",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_forward_confirmation_registry_validation",
        artifact_id_key="registry_id",
        artifact_id=registry_id,
        checks=checks,
    )


def update_confirmation_progress(
    *,
    registry_id: str,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    limited_vs_notrade_dir: Path = DEFAULT_LIMITED_VS_NOTRADE_DIR,
    consensus_risk_dir: Path = DEFAULT_CONSENSUS_RISK_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    registry_payload = confirmation_targets_report_payload(
        registry_id=registry_id,
        output_dir=registry_dir,
    )
    registry = _mapping(registry_payload.get("registered_targets"))
    limited_payload, limited_missing = _latest_limited_payload(limited_vs_notrade_dir)
    consensus_payload, consensus_missing = _latest_consensus_payload(consensus_risk_dir)
    rows = [
        _progress_row(
            target,
            limited_payload=limited_payload,
            limited_missing=limited_missing,
            consensus_payload=consensus_payload,
            consensus_missing=consensus_missing,
            generated=generated,
        )
        for target in _records(registry.get("targets"))
    ]
    summary = _progress_summary(rows, registry_id, generated)
    progress_id = _stable_id("confirmation-progress", registry_id, generated)
    progress_dir = _unique_dir(output_dir / progress_id)
    progress_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_progress_manifest",
        "progress_id": progress_dir.name,
        "registry_id": registry_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "targets_total": len(rows),
        "ready_for_evaluation_count": summary["ready_for_evaluation_count"],
        "insufficient_events_count": summary["insufficient_events_count"],
        "confirmation_progress_manifest_path": str(
            progress_dir / "confirmation_progress_manifest.json"
        ),
        "target_progress_path": str(progress_dir / "target_progress.jsonl"),
        "target_progress_summary_path": str(progress_dir / "target_progress_summary.json"),
        "confirmation_progress_report_path": str(progress_dir / "confirmation_progress_report.md"),
        "market_regime": "ai_after_chatgpt",
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "auto_apply": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(progress_dir / "confirmation_progress_manifest.json", manifest)
    _write_jsonl(progress_dir / "target_progress.jsonl", rows)
    _write_json(progress_dir / "target_progress_summary.json", summary)
    _write_text(
        progress_dir / "confirmation_progress_report.md",
        render_confirmation_progress_report(manifest, rows, summary),
    )
    _update_latest_pointer(
        "latest_confirmation_progress",
        progress_dir.name,
        progress_dir / "confirmation_progress_manifest.json",
    )
    return {
        "progress_id": progress_dir.name,
        "progress_dir": progress_dir,
        "manifest": manifest,
        "target_progress": rows,
        "target_progress_summary": summary,
    }


def confirmation_progress_report_payload(
    *,
    progress_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> dict[str, Any]:
    progress_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=progress_id if not latest else None,
        pointer_name="latest_confirmation_progress",
    )
    return {
        **_read_json(progress_dir / "confirmation_progress_manifest.json"),
        "target_progress": _read_jsonl(progress_dir / "target_progress.jsonl"),
        "target_progress_summary": _read_json(progress_dir / "target_progress_summary.json"),
        "confirmation_progress_report": _read_text(
            progress_dir / "confirmation_progress_report.md"
        ),
        "progress_dir": str(progress_dir),
    }


def validate_confirmation_progress_artifact(
    *,
    progress_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
) -> dict[str, Any]:
    progress_dir = output_dir / progress_id
    manifest = _read_optional_json(progress_dir / "confirmation_progress_manifest.json") or {}
    rows = _read_jsonl(progress_dir / "target_progress.jsonl")
    summary = _read_optional_json(progress_dir / "target_progress_summary.json") or {}
    checks = [
        _check(
            "manifest_exists",
            (progress_dir / "confirmation_progress_manifest.json").exists(),
            "",
        ),
        _check("progress_jsonl_exists", (progress_dir / "target_progress.jsonl").exists(), ""),
        _check("summary_exists", (progress_dir / "target_progress_summary.json").exists(), ""),
        _check("report_exists", (progress_dir / "confirmation_progress_report.md").exists(), ""),
        _check("progress_id_matches", manifest.get("progress_id") == progress_id, ""),
        _check(
            "progress_status_valid",
            all(row.get("progress_status") in PROGRESS_STATUSES for row in rows),
            "progress_status",
        ),
        _check(
            "not_ready_without_required_events",
            all(
                row.get("progress_status") != "READY_FOR_EVALUATION"
                or _available_event_count(row) >= _required_event_count(row)
                for row in rows
            ),
            "required event floor",
        ),
        _check(
            "summary_counts_match",
            _int(summary.get("targets_total")) == len(rows),
            "summary count",
        ),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_progress_validation",
        artifact_id_key="progress_id",
        artifact_id=progress_id,
        checks=checks,
    )


def run_confirmation_evaluation(
    *,
    progress_id: str,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    progress_payload = confirmation_progress_report_payload(
        progress_id=progress_id,
        output_dir=progress_dir,
    )
    rows = [
        _evaluation_row(row, generated=generated)
        for row in _records(progress_payload.get("target_progress"))
    ]
    summary = _evaluation_summary(rows, progress_id, generated)
    evaluation_id = _stable_id("confirmation-evaluation", progress_id, generated)
    evaluation_dir = _unique_dir(output_dir / evaluation_id)
    evaluation_dir.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_evaluation_manifest",
        "evaluation_id": evaluation_dir.name,
        "progress_id": progress_id,
        "registry_id": progress_payload.get("registry_id"),
        "generated_at": generated.isoformat(),
        "status": "PASS" if rows else "FAIL",
        "rule_review_ready": summary["rule_review_ready"],
        "confirmation_evaluation_manifest_path": str(
            evaluation_dir / "confirmation_evaluation_manifest.json"
        ),
        "target_evaluations_path": str(evaluation_dir / "target_evaluations.jsonl"),
        "confirmation_evaluation_summary_path": str(
            evaluation_dir / "confirmation_evaluation_summary.json"
        ),
        "confirmation_evaluation_report_path": str(
            evaluation_dir / "confirmation_evaluation_report.md"
        ),
        "auto_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(evaluation_dir / "confirmation_evaluation_manifest.json", manifest)
    _write_jsonl(evaluation_dir / "target_evaluations.jsonl", rows)
    _write_json(evaluation_dir / "confirmation_evaluation_summary.json", summary)
    _write_text(
        evaluation_dir / "confirmation_evaluation_report.md",
        render_confirmation_evaluation_report(manifest, rows, summary),
    )
    _update_latest_pointer(
        "latest_confirmation_evaluation",
        evaluation_dir.name,
        evaluation_dir / "confirmation_evaluation_manifest.json",
    )
    return {
        "evaluation_id": evaluation_dir.name,
        "evaluation_dir": evaluation_dir,
        "manifest": manifest,
        "target_evaluations": rows,
        "confirmation_evaluation_summary": summary,
    }


def confirmation_evaluation_report_payload(
    *,
    evaluation_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> dict[str, Any]:
    evaluation_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=evaluation_id if not latest else None,
        pointer_name="latest_confirmation_evaluation",
    )
    return {
        **_read_json(evaluation_dir / "confirmation_evaluation_manifest.json"),
        "target_evaluations": _read_jsonl(evaluation_dir / "target_evaluations.jsonl"),
        "confirmation_evaluation_summary": _read_json(
            evaluation_dir / "confirmation_evaluation_summary.json"
        ),
        "confirmation_evaluation_report": _read_text(
            evaluation_dir / "confirmation_evaluation_report.md"
        ),
        "evaluation_dir": str(evaluation_dir),
    }


def validate_confirmation_evaluation_artifact(
    *,
    evaluation_id: str,
    output_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
) -> dict[str, Any]:
    evaluation_dir = output_dir / evaluation_id
    manifest = _read_optional_json(evaluation_dir / "confirmation_evaluation_manifest.json") or {}
    rows = _read_jsonl(evaluation_dir / "target_evaluations.jsonl")
    checks = [
        _check(
            "manifest_exists",
            (evaluation_dir / "confirmation_evaluation_manifest.json").exists(),
            "",
        ),
        _check("evaluations_exists", (evaluation_dir / "target_evaluations.jsonl").exists(), ""),
        _check(
            "summary_exists",
            (evaluation_dir / "confirmation_evaluation_summary.json").exists(),
            "",
        ),
        _check(
            "report_exists",
            (evaluation_dir / "confirmation_evaluation_report.md").exists(),
            "",
        ),
        _check("evaluation_id_matches", manifest.get("evaluation_id") == evaluation_id, ""),
        _check(
            "evaluation_status_valid",
            all(row.get("evaluation_status") in EVALUATION_STATUSES for row in rows),
            "evaluation_status",
        ),
        _check(
            "not_ready_respects_progress",
            all(
                row.get("evaluation_status") == "NOT_READY"
                for row in rows
                if row.get("progress_status") != "READY_FOR_EVALUATION"
            ),
            "not ready",
        ),
        _check("auto_apply_forbidden", manifest.get("auto_apply") is False, "auto apply"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_confirmation_evaluation_validation",
        artifact_id_key="evaluation_id",
        artifact_id=evaluation_id,
        checks=checks,
    )


def run_rule_review_cycle(
    *,
    registry_id: str,
    progress_id: str,
    evaluation_id: str,
    registry_dir: Path = DEFAULT_CONFIRMATION_REGISTRY_DIR,
    progress_dir: Path = DEFAULT_CONFIRMATION_PROGRESS_DIR,
    evaluation_dir: Path = DEFAULT_CONFIRMATION_EVALUATION_DIR,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    registry_payload = confirmation_targets_report_payload(
        registry_id=registry_id,
        output_dir=registry_dir,
    )
    progress_payload = confirmation_progress_report_payload(
        progress_id=progress_id,
        output_dir=progress_dir,
    )
    evaluation_payload = confirmation_evaluation_report_payload(
        evaluation_id=evaluation_id,
        output_dir=evaluation_dir,
    )
    registry_targets = _records(
        _mapping(registry_payload.get("registered_targets")).get("targets")
    )
    progress_rows = {
        _text(row.get("target_id")): row
        for row in _records(progress_payload.get("target_progress"))
    }
    evaluation_rows = {
        _text(row.get("target_id")): row
        for row in _records(evaluation_payload.get("target_evaluations"))
    }
    decision_targets = [
        _rule_review_decision_row(
            target,
            progress=progress_rows.get(_text(target.get("target_id")), {}),
            evaluation=evaluation_rows.get(_text(target.get("target_id")), {}),
        )
        for target in registry_targets
    ]
    cycle_recommendation = _cycle_recommendation(decision_targets)
    cycle_id = _stable_id("rule-review-cycle", registry_id, progress_id, evaluation_id, generated)
    cycle_dir = _unique_dir(output_dir / cycle_id)
    cycle_dir.mkdir(parents=True, exist_ok=False)
    decision_matrix = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_decision_matrix",
        "cycle_id": cycle_dir.name,
        "registry_id": registry_id,
        "progress_id": progress_id,
        "evaluation_id": evaluation_id,
        "targets": decision_targets,
        "cycle_recommendation": cycle_recommendation,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    reader_brief = render_rule_review_reader_brief(decision_matrix)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_review_cycle_manifest",
        "cycle_id": cycle_dir.name,
        "registry_id": registry_id,
        "progress_id": progress_id,
        "evaluation_id": evaluation_id,
        "generated_at": generated.isoformat(),
        "status": "PASS" if decision_targets else "FAIL",
        "cycle_recommendation": cycle_recommendation,
        "targets_requiring_owner_action": sum(
            1 for row in decision_targets if row.get("owner_action_required") is True
        ),
        "policy_change_allowed": False,
        "rule_review_cycle_manifest_path": str(cycle_dir / "rule_review_cycle_manifest.json"),
        "rule_review_decision_matrix_path": str(cycle_dir / "rule_review_decision_matrix.json"),
        "rule_review_cycle_report_path": str(cycle_dir / "rule_review_cycle_report.md"),
        "reader_brief_section_path": str(cycle_dir / "reader_brief_section.md"),
        "auto_apply": False,
        "production_effect": "none",
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_candidate_generated": False,
        "manual_review_required": True,
        "safety": dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY),
        **DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    }
    _write_json(cycle_dir / "rule_review_cycle_manifest.json", manifest)
    _write_json(cycle_dir / "rule_review_decision_matrix.json", decision_matrix)
    _write_text(
        cycle_dir / "rule_review_cycle_report.md",
        render_rule_review_cycle_report(manifest, decision_matrix),
    )
    _write_text(cycle_dir / "reader_brief_section.md", reader_brief)
    _update_latest_pointer(
        "latest_rule_review_cycle",
        cycle_dir.name,
        cycle_dir / "rule_review_cycle_manifest.json",
    )
    return {
        "cycle_id": cycle_dir.name,
        "cycle_dir": cycle_dir,
        "manifest": manifest,
        "rule_review_decision_matrix": decision_matrix,
        "reader_brief_section": reader_brief,
    }


def rule_review_cycle_report_payload(
    *,
    cycle_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> dict[str, Any]:
    cycle_dir = _artifact_dir_from_latest(
        output_dir=output_dir,
        artifact_id=cycle_id if not latest else None,
        pointer_name="latest_rule_review_cycle",
    )
    return {
        **_read_json(cycle_dir / "rule_review_cycle_manifest.json"),
        "rule_review_decision_matrix": _read_json(cycle_dir / "rule_review_decision_matrix.json"),
        "reader_brief_section": _read_text(cycle_dir / "reader_brief_section.md"),
        "rule_review_cycle_report": _read_text(cycle_dir / "rule_review_cycle_report.md"),
        "cycle_dir": str(cycle_dir),
    }


def validate_rule_review_cycle_artifact(
    *,
    cycle_id: str,
    output_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
) -> dict[str, Any]:
    cycle_dir = output_dir / cycle_id
    manifest = _read_optional_json(cycle_dir / "rule_review_cycle_manifest.json") or {}
    matrix = _read_optional_json(cycle_dir / "rule_review_decision_matrix.json") or {}
    targets = _records(matrix.get("targets"))
    checks = [
        _check("manifest_exists", (cycle_dir / "rule_review_cycle_manifest.json").exists(), ""),
        _check("matrix_exists", (cycle_dir / "rule_review_decision_matrix.json").exists(), ""),
        _check("report_exists", (cycle_dir / "rule_review_cycle_report.md").exists(), ""),
        _check("reader_brief_exists", (cycle_dir / "reader_brief_section.md").exists(), ""),
        _check("cycle_id_matches", manifest.get("cycle_id") == cycle_id, ""),
        _check(
            "decision_values_valid",
            all(row.get("rule_review_decision") in RULE_REVIEW_DECISIONS for row in targets),
            "rule_review_decision",
        ),
        _check(
            "policy_change_disallowed",
            manifest.get("policy_change_allowed") is False
            and matrix.get("policy_change_allowed") is False
            and all(row.get("policy_change_allowed") is False for row in targets),
            "policy_change_allowed=false",
        ),
        _check("auto_apply_forbidden", manifest.get("auto_apply") is False, "auto apply"),
        _check(
            "broker_action_forbidden",
            manifest.get("broker_action_allowed") is False
            and manifest.get("broker_action_taken") is False,
            "broker action",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_review_cycle_validation",
        artifact_id_key="cycle_id",
        artifact_id=cycle_id,
        checks=checks,
    )


def create_rule_owner_decision(
    *,
    cycle_id: str,
    cycle_dir: Path = DEFAULT_RULE_REVIEW_CYCLE_DIR,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    cycle_payload = rule_review_cycle_report_payload(cycle_id=cycle_id, output_dir=cycle_dir)
    matrix = _mapping(cycle_payload.get("rule_review_decision_matrix"))
    targets = _records(matrix.get("targets"))
    requiring_owner = [
        _text(row.get("target_id"))
        for row in targets
        if row.get("owner_action_required") is True
    ]
    target_ids = requiring_owner or [_text(row.get("target_id")) for row in targets]
    decision_id = _stable_id("rule-owner-decision", cycle_id, generated)
    record = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_record",
        "decision_id": decision_id,
        "cycle_id": cycle_id,
        "created_at": generated.isoformat(),
        "updated_at": generated.isoformat(),
        "target_ids": target_ids,
        "recommended_cycle_action": _text(matrix.get("cycle_recommendation"), "continue_tracking"),
        "owner_decision": "pending",
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": "none",
        "notes": "",
        "source_cycle_report": cycle_payload.get("rule_review_cycle_report_path"),
    }
    _append_owner_decision(journal_path, record)
    _write_owner_decision_report(journal_path, decision_id=decision_id)
    _update_latest_pointer(
        "latest_rule_owner_decision",
        decision_id,
        journal_path,
    )
    return {
        "decision_id": decision_id,
        "journal_path": journal_path,
        "record": record,
        "records": _read_owner_decisions(journal_path),
    }


def record_rule_owner_decision(
    *,
    decision_id: str,
    decision: Literal[
        "continue_tracking",
        "keep_current_rules",
        "request_more_data",
        "approve_manual_policy_review",
        "reject_rule_change",
        "defer",
    ],
    notes: str = "",
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    if decision not in OWNER_DECISIONS - {"pending"}:
        raise DynamicV3ConfirmationCycleError(f"unsupported owner decision: {decision}")
    generated = generated_at or datetime.now(UTC)
    records = _read_owner_decisions(journal_path)
    updated = False
    for row in records:
        if row.get("decision_id") == decision_id:
            row["owner_decision"] = decision
            row["updated_at"] = generated.isoformat()
            row["notes"] = notes
            row["policy_change_allowed"] = False
            row["auto_apply"] = False
            row["broker_action_allowed"] = False
            row["broker_action_taken"] = False
            row["production_effect"] = "none"
            updated = True
            break
    if not updated:
        raise DynamicV3ConfirmationCycleError(f"owner decision not found: {decision_id}")
    _write_jsonl(journal_path, records)
    _write_owner_decision_report(journal_path, decision_id=decision_id)
    _update_latest_pointer(
        "latest_rule_owner_decision",
        decision_id,
        journal_path,
    )
    return {
        "decision_id": decision_id,
        "journal_path": journal_path,
        "record": next(row for row in records if row.get("decision_id") == decision_id),
        "records": records,
    }


def list_rule_owner_decisions(
    *, journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH
) -> dict[str, Any]:
    records = _read_owner_decisions(journal_path)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_list",
        "status": "PASS" if records else "MISSING",
        "journal_path": str(journal_path),
        "decision_count": len(records),
        "pending_count": sum(1 for row in records if row.get("owner_decision") == "pending"),
        "records": records,
        "production_effect": "none",
        "broker_action_allowed": False,
        "auto_apply": False,
    }


def rule_owner_decision_report_payload(
    *,
    decision_id: str | None = None,
    latest: bool = False,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> dict[str, Any]:
    records = _read_owner_decisions(journal_path)
    if not records:
        raise DynamicV3ConfirmationCycleError(f"owner decision journal not found: {journal_path}")
    record = _select_owner_decision(records, decision_id=decision_id, latest=latest)
    report_path = journal_path.parent / "rule_owner_decision_report.md"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_rule_owner_decision_report",
        "status": "PASS",
        "decision_id": record.get("decision_id"),
        "owner_decision": record.get("owner_decision"),
        "record": record,
        "records": records,
        "journal_path": str(journal_path),
        "rule_owner_decision_report_path": str(report_path),
        "rule_owner_decision_report": _read_text(report_path) if report_path.exists() else "",
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def validate_rule_owner_decision_artifact(
    *,
    decision_id: str,
    journal_path: Path = DEFAULT_RULE_OWNER_DECISION_JOURNAL_PATH,
) -> dict[str, Any]:
    records = _read_owner_decisions(journal_path)
    record = next((row for row in records if row.get("decision_id") == decision_id), {})
    checks = [
        _check("journal_exists", journal_path.exists(), str(journal_path)),
        _check("decision_exists", bool(record), decision_id),
        _check(
            "owner_decision_valid",
            _text(record.get("owner_decision")) in OWNER_DECISIONS,
            "owner_decision",
        ),
        _check("auto_apply_forbidden", record.get("auto_apply") is False, "auto_apply=false"),
        _check(
            "broker_action_forbidden",
            record.get("broker_action_allowed") is False
            and record.get("broker_action_taken") is False,
            "broker action",
        ),
        _check(
            "production_effect_none",
            record.get("production_effect") == "none",
            "production_effect=none",
        ),
        _check(
            "policy_change_disallowed",
            record.get("policy_change_allowed") is False,
            "policy_change_allowed=false",
        ),
    ]
    return _validation_payload(
        report_type="etf_dynamic_v3_rule_owner_decision_validation",
        artifact_id_key="decision_id",
        artifact_id=decision_id,
        checks=checks,
    )


def render_confirmation_registry_report(
    manifest: Mapping[str, Any], registry: Mapping[str, Any]
) -> str:
    targets = _records(registry.get("targets"))
    lines = [
        "# Dynamic Rescue Forward Confirmation Target Registry",
        "",
        f"- registry_id: `{manifest.get('registry_id')}`",
        f"- source_confirmation_plan_id: `{manifest.get('source_confirmation_plan_id')}`",
        "- market_regime: `ai_after_chatgpt`",
        f"- targets_total: {len(targets)}",
        f"- active_target_count: {manifest.get('active_target_count')}",
        f"- watch_only_target_count: {manifest.get('watch_only_target_count')}",
        "- auto_apply: `false`",
        "- owner_approval_required: `true`",
        "- production_effect: `none`",
        "",
        "## Targets",
    ]
    for row in targets:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- status: `{row.get('status')}`",
                f"- current_status: `{row.get('current_status')}`",
                f"- priority: `{row.get('priority')}`",
                f"- windows: `{row.get('windows')}`",
                f"- required_forward_events: `{row.get('required_forward_events', '')}`",
                f"- required_pressure_regime_events: "
                f"`{row.get('required_pressure_regime_events', '')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_confirmation_progress_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Progress",
        "",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- registry_id: `{manifest.get('registry_id')}`",
        "- market_regime: `ai_after_chatgpt`",
        f"- ready_for_evaluation_count: {summary.get('ready_for_evaluation_count')}",
        f"- insufficient_events_count: {summary.get('insufficient_events_count')}",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Target Progress",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- progress_status: `{row.get('progress_status')}`",
                f"- available_forward_events: {row.get('available_forward_events')}",
                f"- required_forward_events: {row.get('required_forward_events')}",
                f"- blocking_reasons: `{row.get('blocking_reasons')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_confirmation_evaluation_report(
    manifest: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> str:
    lines = [
        "# Dynamic Rescue Confirmation Evaluation",
        "",
        f"- evaluation_id: `{manifest.get('evaluation_id')}`",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- success_count: {summary.get('success_count')}",
        f"- failure_count: {summary.get('failure_count')}",
        f"- not_ready_count: {summary.get('not_ready_count')}",
        f"- rule_review_ready: `{summary.get('rule_review_ready')}`",
        f"- summary_recommendation: `{summary.get('summary_recommendation')}`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Target Evaluations",
    ]
    for row in rows:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- evaluation_status: `{row.get('evaluation_status')}`",
                f"- progress_status: `{row.get('progress_status')}`",
                f"- failure_conditions_triggered: `{row.get('failure_conditions_triggered')}`",
                f"- recommendation: `{row.get('recommendation')}`",
            ]
        )
    return "\n".join(lines) + "\n"


def render_rule_review_cycle_report(
    manifest: Mapping[str, Any], decision_matrix: Mapping[str, Any]
) -> str:
    targets = _records(decision_matrix.get("targets"))
    lines = [
        "# Dynamic Rescue Rule Review Cycle",
        "",
        f"- cycle_id: `{manifest.get('cycle_id')}`",
        f"- registry_id: `{manifest.get('registry_id')}`",
        f"- progress_id: `{manifest.get('progress_id')}`",
        f"- evaluation_id: `{manifest.get('evaluation_id')}`",
        f"- cycle_recommendation: `{manifest.get('cycle_recommendation')}`",
        "- policy_change_allowed: `false`",
        "- auto_apply: `false`",
        "- production_effect: `none`",
        "",
        "## Review Questions",
        "",
        f"- active targets: {sum(1 for row in targets if row.get('target_status') == 'active')}",
        f"- ready for evaluation: "
        f"{sum(1 for row in targets if row.get('current_status') == 'SUCCESS')}",
        f"- success reached: {sum(1 for row in targets if row.get('current_status') == 'SUCCESS')}",
        f"- failure triggered: "
        f"{sum(1 for row in targets if row.get('failure_conditions_triggered'))}",
        "- policy change allowed: false",
        "",
        "## Decision Matrix",
    ]
    for row in targets:
        lines.extend(
            [
                "",
                f"### {_text(row.get('target_id'))}",
                f"- current_status: `{row.get('current_status')}`",
                f"- rule_review_decision: `{row.get('rule_review_decision')}`",
                f"- owner_action_required: `{row.get('owner_action_required')}`",
                f"- policy_change_allowed: `{row.get('policy_change_allowed')}`",
                f"- reason: {row.get('reason')}",
            ]
        )
    return "\n".join(lines) + "\n"


def render_rule_review_reader_brief(decision_matrix: Mapping[str, Any]) -> str:
    targets = _records(decision_matrix.get("targets"))
    ready_count = sum(1 for row in targets if row.get("current_status") == "SUCCESS")
    owner_count = sum(1 for row in targets if row.get("owner_action_required") is True)
    return (
        "## Dynamic Rescue Rule Review Cycle\n\n"
        f"- targets_total: {len(targets)}\n"
        f"- ready_for_evaluation_count: {ready_count}\n"
        f"- rule_review_decision: `{decision_matrix.get('cycle_recommendation')}`\n"
        f"- cycle_recommendation: `{decision_matrix.get('cycle_recommendation')}`\n"
        f"- targets_requiring_owner_action: {owner_count}\n"
        "- next_action: `continue_forward_tracking_or_owner_manual_review`\n"
        "- policy_change_allowed: `false`\n"
        "- production_effect: `none`\n"
    )


def render_rule_owner_decision_report(records: Sequence[Mapping[str, Any]]) -> str:
    pending = [row for row in records if row.get("owner_decision") == "pending"]
    latest = records[-1] if records else {}
    lines = [
        "# Dynamic Rescue Rule Owner Decision Journal",
        "",
        f"- decision_count: {len(records)}",
        f"- pending_count: {len(pending)}",
        f"- latest_decision_id: `{latest.get('decision_id', '')}`",
        f"- latest_owner_decision: `{latest.get('owner_decision', '')}`",
        "- auto_apply: `false`",
        "- broker_action_allowed: `false`",
        "- production_effect: `none`",
        "",
        "## Records",
    ]
    for row in records:
        lines.extend(
            [
                "",
                f"### {_text(row.get('decision_id'))}",
                f"- cycle_id: `{row.get('cycle_id')}`",
                f"- owner_decision: `{row.get('owner_decision')}`",
                f"- target_ids: `{row.get('target_ids')}`",
                f"- recommended_cycle_action: `{row.get('recommended_cycle_action')}`",
                f"- notes: {row.get('notes', '')}",
            ]
        )
    return "\n".join(lines) + "\n"


def _registry_target(
    row: Mapping[str, Any], failure_conditions: Mapping[str, Any]
) -> dict[str, Any]:
    target_id = _text(row.get("target_id"))
    status = "watch_only" if target_id == "consensus_target_risk" else "active"
    current_status = "watch_only" if status == "watch_only" else "in_progress"
    failure_map = {
        _text(item.get("condition")): {"action": _text(item.get("action"))}
        for item in _records(failure_conditions.get("failure_conditions"))
        if _text(item.get("target")) == target_id
    }
    result = {
        "target_id": target_id,
        "status": status,
        "priority": _text(row.get("priority"), "MEDIUM"),
        "source": "backtest_sim_forward_confirmation_plan",
        "windows": [_int(item) for item in row.get("windows", [])],
        "success_criteria": _mapping(row.get("success_criteria")),
        "failure_conditions": failure_map,
        "current_status": current_status,
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }
    if "required_pressure_regime_events" in row:
        result["required_pressure_regime_events"] = _int(row.get("required_pressure_regime_events"))
    if "required_forward_events" in row:
        result["required_forward_events"] = _int(row.get("required_forward_events"))
    return result


def _progress_row(
    target: Mapping[str, Any],
    *,
    limited_payload: Mapping[str, Any],
    limited_missing: bool,
    consensus_payload: Mapping[str, Any],
    consensus_missing: bool,
    generated: datetime,
) -> dict[str, Any]:
    target_id = _text(target.get("target_id"))
    if target_id == "limited_adjustment_vs_no_trade":
        metrics, available_by_window = _limited_metrics(limited_payload, _int_windows(target))
        available = sum(available_by_window.values())
        required = _int(target.get("required_forward_events"), 10)
        blocking = _event_blockers(
            available=available,
            required=required,
            available_by_window=available_by_window,
            missing_source=limited_missing,
            missing_source_reason="missing_limited_vs_notrade_artifact",
        )
        status = _progress_status(available, required, available_by_window, blocking)
    elif target_id == "defensive_limited_adjustment_drawdown":
        metrics, available_by_window = _defensive_metrics(limited_payload, _int_windows(target))
        available = 0
        required = _int(target.get("required_pressure_regime_events"), 5)
        blocking = [
            "missing_pressure_regime_forward_events",
            "pressure_regime_tagged_outcomes_required",
        ]
        if limited_missing:
            blocking.append("missing_limited_vs_notrade_artifact")
        status = _progress_status(available, required, available_by_window, blocking)
    else:
        metrics, available_by_window = _consensus_metrics(consensus_payload, _int_windows(target))
        available = sum(available_by_window.values())
        required = _int(target.get("required_forward_events"), 10)
        blocking = _event_blockers(
            available=available,
            required=required,
            available_by_window=available_by_window,
            missing_source=consensus_missing,
            missing_source_reason="missing_consensus_risk_artifact",
        )
        if metrics.get("drawdown_delta_vs_limited_adjustment") is None:
            blocking.append("missing_consensus_vs_limited_adjustment_drawdown_metric")
        status = _progress_status(available, required, available_by_window, blocking)
    return {
        "target_id": target_id,
        "status": target.get("current_status"),
        "target_status": target.get("status"),
        "priority": target.get("priority"),
        "windows": _int_windows(target),
        "required_forward_events": _int(target.get("required_forward_events")),
        "required_pressure_regime_events": _int(target.get("required_pressure_regime_events")),
        "available_forward_events": available,
        "available_pressure_regime_events": (
            available if target_id == "defensive_limited_adjustment_drawdown" else 0
        ),
        "available_by_window": {str(key): value for key, value in available_by_window.items()},
        "current_metrics": metrics,
        "success_criteria": _mapping(target.get("success_criteria")),
        "failure_conditions": _mapping(target.get("failure_conditions")),
        "progress_status": status,
        "blocking_reasons": sorted(set(blocking)),
        "last_updated": generated.isoformat(),
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _progress_summary(
    rows: Sequence[Mapping[str, Any]], registry_id: str, generated: datetime
) -> dict[str, Any]:
    ready = sum(1 for row in rows if row.get("progress_status") == "READY_FOR_EVALUATION")
    blocked = sum(1 for row in rows if row.get("progress_status") == "BLOCKED")
    insufficient = sum(1 for row in rows if row.get("progress_status") == "INSUFFICIENT_EVENTS")
    watch_only = sum(1 for row in rows if row.get("target_status") == "watch_only")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_progress_summary",
        "progress_id": "",
        "registry_id": registry_id,
        "generated_at": generated.isoformat(),
        "targets_total": len(rows),
        "ready_for_evaluation_count": ready,
        "in_progress_count": sum(
            1 for row in rows if row.get("progress_status") in {"IN_PROGRESS", "NEAR_READY"}
        ),
        "blocked_count": blocked,
        "insufficient_events_count": insufficient,
        "watch_only_count": watch_only,
        "summary_recommendation": (
            "run_success_failure_evaluation" if ready else "continue_forward_tracking"
        ),
        "production_effect": "none",
        "broker_action_allowed": False,
    }


def _evaluation_row(row: Mapping[str, Any], *, generated: datetime) -> dict[str, Any]:
    criteria = _mapping(row.get("success_criteria"))
    metrics = _mapping(row.get("current_metrics"))
    criteria_results = {
        name: _criteria_result(name, required, metrics)
        for name, required in criteria.items()
    }
    progress_status = _text(row.get("progress_status"))
    triggered = _failure_conditions_triggered(
        _mapping(row.get("failure_conditions")),
        metrics,
    )
    if progress_status != "READY_FOR_EVALUATION":
        status = "NOT_READY"
        recommendation = "continue_tracking"
    else:
        statuses = [_text(item.get("status")) for item in criteria_results.values()]
        if triggered or "FAIL" in statuses:
            status = "FAILURE"
            recommendation = _failure_recommendation(row, triggered)
        elif statuses and all(item == "PASS" for item in statuses):
            status = "SUCCESS"
            recommendation = "ready_for_rule_review"
        elif "INSUFFICIENT_DATA" in statuses:
            status = "REVIEW_REQUIRED"
            recommendation = "manual_review_required"
        else:
            status = "MIXED"
            recommendation = "manual_review_required"
    return {
        "target_id": row.get("target_id"),
        "evaluation_status": status,
        "progress_status": progress_status,
        "criteria_results": criteria_results,
        "failure_conditions_triggered": triggered,
        "recommendation": recommendation,
        "evaluated_at": generated.isoformat(),
        "owner_approval_required": True,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _evaluation_summary(
    rows: Sequence[Mapping[str, Any]], progress_id: str, generated: datetime
) -> dict[str, Any]:
    counts = {
        status: sum(1 for row in rows if row.get("evaluation_status") == status)
        for status in EVALUATION_STATUSES
    }
    review_ready = counts["SUCCESS"] > 0 or counts["FAILURE"] > 0 or counts["REVIEW_REQUIRED"] > 0
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_confirmation_evaluation_summary",
        "evaluation_id": "",
        "progress_id": progress_id,
        "generated_at": generated.isoformat(),
        "success_count": counts["SUCCESS"],
        "failure_count": counts["FAILURE"],
        "mixed_count": counts["MIXED"],
        "not_ready_count": counts["NOT_READY"],
        "review_required_count": counts["REVIEW_REQUIRED"],
        "rule_review_ready": review_ready,
        "summary_recommendation": (
            "prepare_rule_review_cycle" if review_ready else "continue_forward_tracking"
        ),
        "auto_apply": False,
        "production_effect": "none",
    }


def _rule_review_decision_row(
    target: Mapping[str, Any],
    *,
    progress: Mapping[str, Any],
    evaluation: Mapping[str, Any],
) -> dict[str, Any]:
    target_id = _text(target.get("target_id"))
    evaluation_status = _text(evaluation.get("evaluation_status"), "NOT_READY")
    failures = _records(evaluation.get("failure_conditions_triggered"))
    if target.get("status") == "watch_only":
        decision = "KEEP_REFERENCE_ONLY"
        reason = "Consensus target remains a risk watch item and reference only."
        owner_action = False
    elif evaluation_status == "SUCCESS":
        decision = "READY_FOR_OWNER_REVIEW"
        reason = "Forward evidence satisfies registry criteria; manual rule review is allowed."
        owner_action = True
    elif evaluation_status == "FAILURE":
        if target_id == "defensive_limited_adjustment_drawdown":
            decision = "RENAME_OR_RECLASSIFY"
        else:
            decision = "TIGHTEN_RULES_RECOMMENDED"
        reason = "Failure condition triggered or criteria failed; do not loosen rules."
        owner_action = True
    elif evaluation_status in {"MIXED", "REVIEW_REQUIRED"}:
        decision = "DEFER"
        reason = "Evidence is mixed or incomplete; defer rule mutation."
        owner_action = False
    else:
        decision = "CONTINUE_TRACKING"
        available = _available_event_count(progress)
        required = _required_event_count(progress)
        reason = f"Only {available} eligible events are available; {required} required."
        owner_action = False
    return {
        "target_id": target_id,
        "target_status": target.get("status"),
        "current_status": evaluation_status,
        "progress_status": progress.get("progress_status"),
        "rule_review_decision": decision,
        "reason": reason,
        "failure_conditions_triggered": failures,
        "owner_action_required": owner_action,
        "policy_change_allowed": False,
        "auto_apply": False,
        "broker_action_allowed": False,
        "production_effect": "none",
    }


def _cycle_recommendation(rows: Sequence[Mapping[str, Any]]) -> str:
    if any(row.get("owner_action_required") is True for row in rows):
        return "owner_manual_review_required"
    if any(row.get("rule_review_decision") == "DEFER" for row in rows):
        return "defer_until_more_evidence"
    return "continue_tracking"


def _latest_limited_payload(output_dir: Path) -> tuple[dict[str, Any], bool]:
    try:
        latest = _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name="latest_limited_vs_notrade",
        )
    except Exception:  # noqa: BLE001
        return {}, True
    return {
        **(_read_optional_json(latest / "limited_vs_notrade_manifest.json") or {}),
        "sample_inventory": _read_jsonl(latest / "sample_inventory.jsonl"),
        "window_comparison_metrics": _read_optional_json(latest / "window_comparison_metrics.json")
        or {},
    }, False


def _latest_consensus_payload(output_dir: Path) -> tuple[dict[str, Any], bool]:
    try:
        latest = _artifact_dir_from_latest(
            output_dir=output_dir,
            artifact_id=None,
            pointer_name="latest_consensus_risk",
        )
    except Exception:  # noqa: BLE001
        return {}, True
    return {
        **(_read_optional_json(latest / "consensus_risk_manifest.json") or {}),
        "consensus_exposure_summary": _read_optional_json(
            latest / "consensus_exposure_summary.json"
        )
        or {},
        "consensus_drawdown_risk": _read_optional_json(latest / "consensus_drawdown_risk.json")
        or {},
        "consensus_turnover_risk": _read_optional_json(latest / "consensus_turnover_risk.json")
        or {},
    }, False


def _limited_metrics(
    payload: Mapping[str, Any], windows: Sequence[int]
) -> tuple[dict[str, Any], dict[int, int]]:
    by_window = _records(_mapping(payload.get("window_comparison_metrics")).get("by_window"))
    selected = [row for row in by_window if _int(row.get("window_days")) in set(windows)]
    available_by_window = {
        window: sum(
            _int(row.get("available_count"))
            for row in selected
            if _int(row.get("window_days")) == window
        )
        for window in windows
    }
    weights = [_int(row.get("available_count")) for row in selected]
    return {
        "win_rate_vs_no_trade": _weighted_avg(selected, "win_rate", weights),
        "avg_relative_return": _weighted_avg(selected, "avg_relative_return", weights),
        "drawdown_delta": _weighted_avg(selected, "avg_drawdown_delta", weights),
    }, available_by_window


def _defensive_metrics(
    payload: Mapping[str, Any], windows: Sequence[int]
) -> tuple[dict[str, Any], dict[int, int]]:
    metrics, available = _limited_metrics(payload, windows)
    return {
        "drawdown_delta_vs_no_trade": metrics.get("drawdown_delta"),
        "win_rate_vs_no_trade": metrics.get("win_rate_vs_no_trade"),
        "pressure_regime_sample_status": "MISSING_PRESSURE_REGIME_TAGS",
    }, available


def _consensus_metrics(
    payload: Mapping[str, Any], windows: Sequence[int]
) -> tuple[dict[str, Any], dict[int, int]]:
    drawdown_rows = _records(_mapping(payload.get("consensus_drawdown_risk")).get("window_results"))
    available_by_window = {
        window: sum(
            _int(row.get("available_count"))
            for row in drawdown_rows
            if _int(row.get("window_days")) == window
        )
        for window in windows
    }
    selected = [row for row in drawdown_rows if _int(row.get("window_days")) in set(windows)]
    weights = [_int(row.get("available_count")) for row in selected]
    turnover = _mapping(payload.get("consensus_turnover_risk"))
    return {
        "drawdown_delta_vs_limited_adjustment": None,
        "drawdown_delta_vs_no_trade": _weighted_avg(
            selected, "drawdown_delta_vs_no_trade", weights
        ),
        "turnover_delta": _float(turnover.get("avg_turnover")),
        "consensus_target_risk": _text(payload.get("consensus_target_risk"), "INSUFFICIENT_DATA"),
    }, available_by_window


def _event_blockers(
    *,
    available: int,
    required: int,
    available_by_window: Mapping[int, int],
    missing_source: bool,
    missing_source_reason: str,
) -> list[str]:
    reasons = []
    if missing_source:
        reasons.append(missing_source_reason)
    if available < required:
        reasons.append("not_enough_forward_events")
    missing_windows = [str(window) for window, count in available_by_window.items() if count <= 0]
    if missing_windows:
        reasons.append("missing_" + "_".join(missing_windows) + "d_windows")
    return reasons


def _progress_status(
    available: int,
    required: int,
    available_by_window: Mapping[int, int],
    blocking: Sequence[str],
) -> str:
    if any(reason.startswith("invalid_") for reason in blocking):
        return "BLOCKED"
    all_windows_present = all(count > 0 for count in available_by_window.values())
    if available >= required and all_windows_present and not blocking:
        return "READY_FOR_EVALUATION"
    if available >= max(1, int(required * 0.8)):
        return "NEAR_READY"
    if available > 0:
        return "IN_PROGRESS"
    return "INSUFFICIENT_EVENTS"


def _criteria_result(
    name: str, required: Any, metrics: Mapping[str, Any]
) -> dict[str, Any]:
    actual = _metric_actual(name, metrics)
    if actual is None:
        status = "INSUFFICIENT_DATA"
    elif name.endswith("_min"):
        status = "PASS" if _float(actual) >= _float(required) else "FAIL"
    elif name.endswith("_max"):
        status = "PASS" if _float(actual) <= _float(required) else "FAIL"
    else:
        status = "REVIEW_REQUIRED"
    return {"required": required, "actual": actual, "status": status}


def _metric_actual(name: str, metrics: Mapping[str, Any]) -> Any:
    mapping = {
        "win_rate_vs_no_trade_min": "win_rate_vs_no_trade",
        "avg_relative_return_min": "avg_relative_return",
        "drawdown_delta_max": "drawdown_delta",
        "drawdown_delta_vs_no_trade_max": "drawdown_delta_vs_no_trade",
        "drawdown_delta_vs_limited_adjustment_max": "drawdown_delta_vs_limited_adjustment",
        "turnover_delta_max": "turnover_delta",
    }
    key = mapping.get(name, name.removesuffix("_min").removesuffix("_max"))
    return metrics.get(key)


def _failure_conditions_triggered(
    failure_conditions: Mapping[str, Any], metrics: Mapping[str, Any]
) -> list[dict[str, Any]]:
    triggered = []
    for condition, payload in failure_conditions.items():
        action = _text(_mapping(payload).get("action"))
        if condition == "underperforms_no_trade" and _float(
            metrics.get("avg_relative_return")
        ) < 0:
            triggered.append({"condition": condition, "action": action})
        elif condition in {
            "drawdown_worsening_persists",
            "fails_to_reduce_drawdown_in_pressure_regime",
            "excess_drawdown_persists",
        }:
            drawdown = (
                metrics.get("drawdown_delta")
                if metrics.get("drawdown_delta") is not None
                else metrics.get("drawdown_delta_vs_no_trade")
            )
            if drawdown is not None and _float(drawdown) > 0:
                triggered.append({"condition": condition, "action": action})
    return triggered


def _failure_recommendation(row: Mapping[str, Any], triggered: Sequence[Mapping[str, Any]]) -> str:
    target_id = _text(row.get("target_id"))
    actions = {_text(item.get("action")) for item in triggered}
    if "tighten_or_disable_limited_adjustment_proposal" in actions:
        return "tighten_rules"
    if target_id == "defensive_limited_adjustment_drawdown":
        return "do_not_loosen_rules"
    return "manual_review_required"


def _weighted_avg(
    rows: Sequence[Mapping[str, Any]], key: str, weights: Sequence[int]
) -> float | None:
    pairs = [
        (_float(row.get(key)), weight)
        for row, weight in zip(rows, weights, strict=False)
        if weight > 0 and row.get(key) is not None
    ]
    total = sum(weight for _, weight in pairs)
    if total <= 0:
        return None
    return round(sum(value * weight for value, weight in pairs) / total, 6)


def _required_event_count(row: Mapping[str, Any]) -> int:
    pressure = _int(row.get("required_pressure_regime_events"))
    forward = _int(row.get("required_forward_events"))
    return pressure or forward


def _available_event_count(row: Mapping[str, Any]) -> int:
    pressure = _int(row.get("available_pressure_regime_events"))
    forward = _int(row.get("available_forward_events"))
    return pressure or forward


def _int_windows(target: Mapping[str, Any]) -> list[int]:
    raw = target.get("windows")
    if not isinstance(raw, Sequence) or isinstance(raw, str | bytes):
        return []
    return [_int(item) for item in raw]


def _read_owner_decisions(path: Path) -> list[dict[str, Any]]:
    return _read_jsonl(path)


def _append_owner_decision(path: Path, record: Mapping[str, Any]) -> None:
    records = _read_owner_decisions(path)
    records.append(dict(record))
    _write_jsonl(path, records)


def _write_owner_decision_report(path: Path, *, decision_id: str) -> None:
    _ = decision_id
    records = _read_owner_decisions(path)
    _write_text(
        path.parent / "rule_owner_decision_report.md",
        render_rule_owner_decision_report(records),
    )


def _select_owner_decision(
    records: Sequence[Mapping[str, Any]], *, decision_id: str | None, latest: bool
) -> dict[str, Any]:
    if decision_id is not None and latest:
        raise DynamicV3ConfirmationCycleError("--decision-id and --latest cannot be combined")
    if decision_id is not None:
        for row in records:
            if row.get("decision_id") == decision_id:
                return dict(row)
        raise DynamicV3ConfirmationCycleError(f"owner decision not found: {decision_id}")
    return dict(records[-1])


def _write_yaml(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(_jsonable(payload), sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise DynamicV3ConfirmationCycleError(f"required YAML artifact not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, Mapping):
        raise DynamicV3ConfirmationCycleError(f"YAML artifact root must be mapping: {path}")
    return dict(raw)


def _read_yaml_optional(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return _read_yaml(path)


def _read_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def _jsonable(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return [_jsonable(item) for item in value]
    if isinstance(value, datetime):
        return value.isoformat()
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value
