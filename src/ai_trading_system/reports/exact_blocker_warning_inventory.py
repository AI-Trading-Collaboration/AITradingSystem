from __future__ import annotations

import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.reports.normal_paper_shadow_observation_clock import (
    latest_normal_paper_shadow_observation_clock_json_path,
)
from ai_trading_system.reports.post_recovery_governance_pack import (
    latest_post_recovery_governance_pack_json_path,
)
from ai_trading_system.reports.recovery_triage import (
    latest_recovery_blocker_triage_json_path,
    latest_recovery_owner_action_map_json_path,
    latest_recovery_pack_source_depth_audit_json_path,
    latest_report_index_warning_triage_json_path,
)
from ai_trading_system.reports.research_monthly_review_pack import (
    PRODUCTION_EFFECT,
    _int,
    _latest_dated_path,
    _mapping,
    _md_cell,
    _read_json_mapping,
    _records,
    _text,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "exact_blocker_warning_inventory"
VALIDATION_REPORT_TYPE = "exact_blocker_warning_inventory_validation"

INVENTORY_BLOCKED = "EXACT_INVENTORY_BLOCKED"
INVENTORY_CLEAR = "EXACT_INVENTORY_CLEAR"

PASS_STATUS = "PASS"
PASS_WITH_WARNINGS_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"

CORE_READER_BRIEF_FIELDS = (
    "summary",
    "key_result",
    "blocking_issues",
    "warnings",
    "safety_boundary",
    "next_action",
)

BLOCKER_KIND_BY_SOURCE_ID = {
    "normal_paper_shadow_resumption_gate": "owner",
    "cost_sensitivity_metrics": "metric",
    "benchmark_baseline_metrics": "metric",
    "monthly_review": "owner",
    "promotion_board": "owner",
    "observation_clock": "observation-clock",
    "extended_shadow_protocol": "observation-clock",
    "roadmap_dashboard": "owner",
    "decision_snapshot_lifecycle": "snapshot",
}

UPSTREAM_DEPENDENCY_BY_SOURCE_ID = {
    "normal_paper_shadow_resumption_gate": "owner_manual_review_and_recovery_source_chain",
    "cost_sensitivity_metrics": "candidate_cost_metrics_and_cost_sensitivity_review",
    "benchmark_baseline_metrics": "candidate_benchmark_metrics_and_baseline_control",
    "monthly_review": "owner_manual_review_and_recovery_source_chain",
    "promotion_board": "owner_manual_review_and_recovery_source_chain",
    "observation_clock": "valid_normal_paper_shadow_observation_days",
    "extended_shadow_protocol": "minimum_observation_period_and_all_extension_evidence",
    "roadmap_dashboard": "review_top_roadmap_blockers",
    "decision_snapshot_lifecycle": "canonical_daily_score_decision_snapshot",
}

WARNING_SCOPE_BY_SOURCE_ID = {
    "signal_input_restoration": "data-quality",
    "signal_completeness_recovery": "data-quality",
    "readiness_health_recovery": "owner-review",
    "recovery_evidence_pack": "data-quality",
    "monthly_review": "owner-review",
    "promotion_board": "owner-review",
    "observation_clock": "owner-review",
    "extended_shadow_protocol": "owner-review",
    "roadmap_dashboard": "owner-review",
    "decision_snapshot_lifecycle": "report-index",
}


def default_exact_blocker_warning_inventory_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"exact_blocker_warning_inventory_{as_of.isoformat()}.json"


def default_exact_blocker_warning_inventory_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"exact_blocker_warning_inventory_{as_of.isoformat()}.md"


def default_exact_blocker_warning_inventory_validation_json_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"exact_blocker_warning_inventory_validation_{as_of.isoformat()}.json"


def default_exact_blocker_warning_inventory_validation_markdown_path(
    output_dir: Path,
    as_of: date,
) -> Path:
    return output_dir / f"exact_blocker_warning_inventory_validation_{as_of.isoformat()}.md"


def latest_exact_blocker_warning_inventory_json_path(output_dir: Path) -> Path | None:
    return _latest_dated_path(output_dir, "exact_blocker_warning_inventory_", ".json")


def build_exact_blocker_warning_inventory_payload(
    *,
    as_of: date,
    blocker_triage_payload: Mapping[str, Any] | None = None,
    blocker_triage_path: Path | None = None,
    post_recovery_pack_payload: Mapping[str, Any] | None = None,
    post_recovery_pack_path: Path | None = None,
    source_depth_audit_payload: Mapping[str, Any] | None = None,
    source_depth_audit_path: Path | None = None,
    report_index_warning_triage_payload: Mapping[str, Any] | None = None,
    report_index_warning_triage_path: Path | None = None,
    normal_observation_clock_payload: Mapping[str, Any] | None = None,
    normal_observation_clock_path: Path | None = None,
    owner_action_map_payload: Mapping[str, Any] | None = None,
    owner_action_map_path: Path | None = None,
    reports_dir: Path = PROJECT_ROOT / "outputs" / "reports",
) -> dict[str, Any]:
    blocker_triage_path, blocker_triage = _payload_or_latest(
        blocker_triage_payload,
        blocker_triage_path,
        latest_recovery_blocker_triage_json_path,
        reports_dir,
        "recovery_blocker_triage",
    )
    post_recovery_pack_path, post_recovery_pack = _payload_or_latest(
        post_recovery_pack_payload,
        post_recovery_pack_path,
        latest_post_recovery_governance_pack_json_path,
        reports_dir,
        "post_recovery_governance_pack",
    )
    source_depth_audit_path, source_depth_audit = _payload_or_latest(
        source_depth_audit_payload,
        source_depth_audit_path,
        latest_recovery_pack_source_depth_audit_json_path,
        reports_dir,
        "recovery_pack_source_depth_audit",
    )
    report_index_warning_triage_path, report_index_warning_triage = _payload_or_latest(
        report_index_warning_triage_payload,
        report_index_warning_triage_path,
        latest_report_index_warning_triage_json_path,
        reports_dir,
        "report_index_warning_triage",
    )
    normal_observation_clock_path, normal_observation_clock = _payload_or_latest(
        normal_observation_clock_payload,
        normal_observation_clock_path,
        latest_normal_paper_shadow_observation_clock_json_path,
        reports_dir,
        "normal_paper_shadow_observation_clock",
    )
    owner_action_map_path, owner_action_map = _payload_or_latest(
        owner_action_map_payload,
        owner_action_map_path,
        latest_recovery_owner_action_map_json_path,
        reports_dir,
        "recovery_owner_action_map",
    )

    source_by_id = _source_depth_by_id(source_depth_audit)
    triage_by_issue_id = {
        _text(row.get("issue_id")): row for row in _records(blocker_triage.get("blocker_triage"))
    }
    report_index_warnings = [
        _report_index_warning_row(row)
        for row in _records(report_index_warning_triage.get("warning_triage"))
    ]
    blockers = [
        _blocker_row(row, triage_by_issue_id, source_by_id)
        for row in _records(post_recovery_pack.get("remaining_blockers"))
    ]
    warnings = [
        _warning_row(row, source_by_id, report_index_warnings)
        for row in _records(post_recovery_pack.get("remaining_warnings"))
    ]
    summary = _summary(
        post_recovery_pack=post_recovery_pack,
        blocker_inventory=blockers,
        warning_inventory=warnings,
        report_index_warning_inventory=report_index_warnings,
        normal_observation_clock=normal_observation_clock,
        owner_action_map=owner_action_map,
    )
    status = INVENTORY_BLOCKED if blockers else INVENTORY_CLEAR
    reader_brief = _reader_brief(status, summary, blockers, warnings, report_index_warnings)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": status,
        "inventory_status": status,
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "inventory_only": True,
        "input_artifacts": {
            "recovery_blocker_triage": _path_text(blocker_triage_path),
            "post_recovery_governance_pack": _path_text(post_recovery_pack_path),
            "recovery_pack_source_depth_audit": _path_text(source_depth_audit_path),
            "report_index_warning_triage": _path_text(report_index_warning_triage_path),
            "normal_paper_shadow_observation_clock": _path_text(normal_observation_clock_path),
            "recovery_owner_action_map": _path_text(owner_action_map_path),
        },
        "summary": summary,
        "blocker_inventory": blockers,
        "warning_inventory": warnings,
        "report_index_warning_inventory": report_index_warnings,
        "normal_paper_shadow_boundary": dict(
            _mapping(post_recovery_pack.get("normal_paper_shadow_boundary"))
        ),
        "extended_shadow_boundary": dict(
            _mapping(post_recovery_pack.get("extended_shadow_boundary"))
        ),
        "live_trading_boundary": dict(_mapping(post_recovery_pack.get("live_trading_boundary"))),
        "reader_brief": reader_brief,
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Inventory is read-only and does not resolve blockers or warnings.",
            "It does not regenerate signal inputs, metrics, report-index artifacts, or snapshots.",
            "It does not create waivers or append owner decisions.",
            "Extended shadow and live trading remain forbidden.",
        ],
        "next_action": reader_brief["next_action"],
    }


def validate_exact_blocker_warning_inventory_payload(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []
    blockers = _records(payload.get("blocker_inventory"))
    warnings = _records(payload.get("warning_inventory"))
    report_index_warnings = _records(payload.get("report_index_warning_inventory"))
    summary = _mapping(payload.get("summary"))

    _append_check(
        checks,
        blocking_issues,
        "report_type",
        _text(payload.get("report_type")) == REPORT_TYPE,
        f"report_type must be {REPORT_TYPE}.",
        "regenerate_exact_blocker_warning_inventory",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_count_matches",
        _int(summary.get("blocker_count")) == len(blockers),
        "blocker_count must match blocker_inventory rows.",
        "regenerate_exact_blocker_warning_inventory",
    )
    _append_check(
        checks,
        blocking_issues,
        "warning_count_matches",
        _int(summary.get("warning_count")) == len(warnings),
        "warning_count must match warning_inventory rows.",
        "regenerate_exact_blocker_warning_inventory",
    )
    _append_check(
        checks,
        blocking_issues,
        "source_recovery_counts_match",
        len(blockers) == _int(summary.get("source_post_recovery_blocker_count"))
        and len(warnings) == _int(summary.get("source_post_recovery_warning_count")),
        "Inventory counts must match the current post-recovery source counts.",
        "rerun_exact_inventory_after_recovery_pack_refresh",
    )
    _append_check(
        checks,
        blocking_issues,
        "blocker_required_fields",
        all(_blocker_row_complete(row) for row in blockers),
        "Every blocker must expose source, artifact, dependency, kind, action, and boundaries.",
        "repair_exact_blocker_inventory_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "warning_required_fields",
        all(_warning_row_complete(row) for row in warnings),
        "Every warning must expose source, scope, waiver, and repair requirements.",
        "repair_exact_warning_inventory_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "report_index_warning_required_fields",
        all(_report_index_warning_row_complete(row) for row in report_index_warnings),
        "Every report-index warning must expose affected report and repair type.",
        "repair_report_index_warning_inventory_fields",
    )
    _append_check(
        checks,
        blocking_issues,
        "reader_brief_core_fields",
        _reader_brief_complete(payload.get("reader_brief")),
        "Reader Brief section must expose core fields.",
        "restore_exact_inventory_reader_brief",
    )
    _append_check(
        checks,
        blocking_issues,
        "safety_boundary",
        _safety_boundary_valid(payload.get("safety_boundary")),
        "Inventory must preserve read-only recovery safety boundary.",
        "restore_exact_inventory_safety_boundary",
    )
    if blockers or warnings or report_index_warnings:
        warning_issues.append(
            {
                "issue_id": "inventory_contains_open_items",
                "message": (
                    "Exact inventory remains blocked because open blockers or warnings exist."
                ),
                "blocker_count": len(blockers),
                "warning_count": len(warnings),
                "report_index_warning_count": len(report_index_warnings),
                "recommended_action": _text(payload.get("next_action")),
            }
        )

    validation_status = FAIL_STATUS
    if not blocking_issues:
        validation_status = PASS_WITH_WARNINGS_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of"), date.today().isoformat()),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "status": validation_status,
        "validation_status": validation_status,
        "source_inventory_status": _text(payload.get("inventory_status")),
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "input_artifacts": dict(_mapping(payload.get("input_artifacts"))),
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check["status"] == FAIL_STATUS]
            ),
            "warning_check_count": len(warning_issues),
            "blocker_count": len(blockers),
            "warning_count": len(warnings),
            "report_index_warning_count": len(report_index_warnings),
        },
        "checks": checks,
        "blocking_issues": blocking_issues,
        "warning_issues": warning_issues,
        "reader_brief": {
            "summary": (
                f"Exact inventory validation {validation_status}; "
                f"blockers={len(blockers)}, warnings={len(warnings)}, "
                f"report-index warnings={len(report_index_warnings)}."
            ),
            "key_result": validation_status,
            "blocking_issues": _issue_list(blocking_issues, "issue_id", "message"),
            "warnings": _issue_list(warning_issues, "issue_id", "message"),
            "safety_boundary": "Validation reads generated inventory only; production_effect=none.",
            "next_action": _text(payload.get("next_action")),
            "production_effect": PRODUCTION_EFFECT,
        },
        "safety_boundary": _safety_boundary(),
        "limitations": ["Validation does not resolve recovery blockers."],
        "next_action": _text(payload.get("next_action")),
    }


def write_exact_blocker_warning_inventory_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_exact_blocker_warning_inventory_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(render_exact_blocker_warning_inventory_markdown(payload), output_path)


def write_exact_blocker_warning_inventory_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_json(payload, output_path)


def write_exact_blocker_warning_inventory_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return _write_text(
        render_exact_blocker_warning_inventory_validation_markdown(payload),
        output_path,
    )


def render_exact_blocker_warning_inventory_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Exact Blocker and Warning Inventory",
        "",
        f"- 日期：{_text(payload.get('as_of'))}",
        f"- 状态：{_text(payload.get('inventory_status'))}",
        f"- blocker 数：{_int(summary.get('blocker_count'))}",
        f"- recovery warning 数：{_int(summary.get('warning_count'))}",
        f"- report-index warning 数：{_int(summary.get('report_index_warning_count'))}",
        f"- normal paper-shadow may resume：{summary.get('normal_paper_shadow_may_resume')}",
        f"- extended shadow remains forbidden：{summary.get('extended_shadow_remains_forbidden')}",
        f"- live trading remains forbidden：{summary.get('live_trading_remains_forbidden')}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        "",
        "## Blockers",
        "",
        "|Blocker|Source report|Artifact|Dependency|Kind|Next action|Normal|Extended|Live|",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in _records(payload.get("blocker_inventory")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(row.get(field))
                for field in (
                    "blocker_id",
                    "source_report",
                    "source_artifact_id",
                    "upstream_artifact_dependency",
                    "blocker_kind",
                    "exact_next_action_required",
                    "blocks_normal_paper_shadow",
                    "blocks_extended_shadow",
                    "blocks_live_trading",
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Recovery Warnings",
            "",
            (
                "|Warning|Source report|Scope|Waivable|Code repair|Metadata repair|"
                "Data regeneration|Owner review|Next action|"
            ),
            "|---|---|---|---|---|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("warning_inventory")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(row.get(field))
                for field in (
                    "warning_id",
                    "source_report",
                    "warning_scope",
                    "waivable",
                    "needs_code_repair",
                    "needs_metadata_repair",
                    "needs_data_regeneration",
                    "needs_owner_review",
                    "exact_next_action_required",
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Report Index Warnings",
            "",
            "|Warning|Report|Scope|Repair type|Waivable|Next action|",
            "|---|---|---|---|---|---|",
        ]
    )
    for row in _records(payload.get("report_index_warning_inventory")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(row.get(field))
                for field in (
                    "warning_id",
                    "report_id",
                    "warning_scope",
                    "repair_type",
                    "waivable",
                    "exact_next_action_required",
                )
            )
            + "|"
        )
    lines.extend(
        [
            "",
            "## Reader Brief",
            "",
            _text(_mapping(payload.get("reader_brief")).get("summary")),
            "",
            "## Safety Boundary",
            "",
            (
                "- 只读 inventory，不运行上游，不刷新数据，不补造 artifact，"
                "不创建 waiver，不写 owner decision。"
            ),
            "- Extended shadow 和 live trading 继续 forbidden。",
        ]
    )
    return "\n".join(lines) + "\n"


def render_exact_blocker_warning_inventory_validation_markdown(
    payload: Mapping[str, Any],
) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        "# Exact Blocker and Warning Inventory Validation",
        "",
        f"- 日期：{_text(payload.get('as_of'))}",
        f"- 状态：{_text(payload.get('validation_status'))}",
        f"- checks：{_int(summary.get('check_count'))}",
        f"- failed：{_int(summary.get('failed_check_count'))}",
        f"- warnings：{_int(summary.get('warning_check_count'))}",
        f"- production_effect：{_text(payload.get('production_effect'))}",
        "",
        "## Checks",
        "",
        "|Check|Status|Message|Action|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            "|"
            + "|".join(
                _md_cell(check.get(field))
                for field in ("check_id", "status", "message", "recommended_action")
            )
            + "|"
        )
    return "\n".join(lines) + "\n"


def _payload_or_latest(
    payload: Mapping[str, Any] | None,
    path: Path | None,
    latest_fn: Callable[[Path], Path | None],
    reports_dir: Path,
    label: str,
) -> tuple[Path | None, Mapping[str, Any]]:
    if payload is not None:
        return path, payload
    source_path = path or latest_fn(reports_dir)
    if source_path is None:
        raise FileNotFoundError(f"Missing required {label} artifact under {reports_dir}")
    return source_path, _read_json_mapping(source_path)


def _source_depth_by_id(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    for row in _records(payload.get("source_depth")) + _records(payload.get("unhealthy_sources")):
        source_id = _text(row.get("source_id"))
        if source_id and source_id not in result:
            result[source_id] = row
    return result


def _blocker_row(
    blocker: Mapping[str, Any],
    triage_by_issue_id: Mapping[str, Mapping[str, Any]],
    source_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    blocker_id = _text(blocker.get("issue_id"))
    source_id = _text(blocker.get("source_id"))
    triage = triage_by_issue_id.get(blocker_id, {})
    source = source_by_id.get(source_id, {})
    blocker_kind = BLOCKER_KIND_BY_SOURCE_ID.get(source_id, "owner")
    source_artifact_path = _text(
        blocker.get("artifact_path"),
        _text(source.get("source_payload_path")),
    )
    return {
        "blocker_id": blocker_id,
        "source_report": _text(source.get("source_report"), _text(triage.get("source_report"))),
        "source_artifact_id": _text(source.get("source_artifact_id"), source_id),
        "source_artifact_path": source_artifact_path,
        "upstream_artifact_dependency": UPSTREAM_DEPENDENCY_BY_SOURCE_ID.get(
            source_id,
            _text(source.get("next_required_action"), _text(blocker.get("recommended_action"))),
        ),
        "upstream_artifact_path": _text(
            source.get("validation_payload_path"),
            source_artifact_path,
        ),
        "blocker_kind": blocker_kind,
        "source_status": _text(blocker.get("source_status"), _text(source.get("source_status"))),
        "validation_status": _text(
            source.get("validation_status"),
            _text(blocker.get("validation_status")),
        ),
        "exact_next_action_required": _text(
            source.get("next_required_action"),
            _text(blocker.get("recommended_action")),
        ),
        "blocks_normal_paper_shadow": True,
        "blocks_extended_shadow": True,
        "blocks_live_trading": False,
        "live_trading_note": "Live trading remains forbidden by global boundary, not by this row.",
        "production_effect": PRODUCTION_EFFECT,
    }


def _warning_row(
    warning: Mapping[str, Any],
    source_by_id: Mapping[str, Mapping[str, Any]],
    report_index_warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    source_id = _text(warning.get("source_id"))
    source = source_by_id.get(source_id, {})
    scope = WARNING_SCOPE_BY_SOURCE_ID.get(source_id, "owner-review")
    next_action = _text(
        source.get("next_required_action"),
        _text(warning.get("recommended_action"), "review_recovery_warning"),
    )
    linked_report_index_warning_ids = [
        _text(row.get("warning_id"))
        for row in report_index_warnings
        if _warning_related_to_report_index_warning(source_id, row)
    ]
    return {
        "warning_id": _text(warning.get("issue_id")),
        "source_report": _text(source.get("source_report"), source_id),
        "source_artifact_path": _text(
            warning.get("artifact_path"),
            _text(source.get("source_payload_path")),
        ),
        "warning_scope": scope,
        "metadata_only": scope == "metadata-only",
        "legacy_only": scope == "legacy-only",
        "data_quality_related": scope == "data-quality",
        "report_index_related": scope == "report-index",
        "owner_review_related": scope == "owner-review",
        "waivable": False,
        "needs_code_repair": False,
        "needs_metadata_repair": scope == "metadata-only",
        "needs_data_regeneration": scope in {"data-quality", "report-index"},
        "needs_owner_review": scope == "owner-review",
        "repair_type": _repair_type_for_warning(scope),
        "source_status": _text(warning.get("source_status"), _text(source.get("source_status"))),
        "validation_status": _text(
            warning.get("validation_status"),
            _text(source.get("validation_status")),
        ),
        "exact_next_action_required": next_action,
        "linked_report_index_warning_ids": linked_report_index_warning_ids,
        "production_effect": PRODUCTION_EFFECT,
    }


def _warning_related_to_report_index_warning(
    source_id: str,
    report_index_warning: Mapping[str, Any],
) -> bool:
    report_id = _text(report_index_warning.get("report_id"))
    if source_id == "decision_snapshot_lifecycle":
        return report_id in {"daily_score", "calculation_explainers", "score_change_attribution"}
    if source_id == "recovery_evidence_pack":
        return report_id in {"evidence_dashboard", "market_panel", "market_data_freshness"}
    if source_id in {"monthly_review", "promotion_board", "roadmap_dashboard"}:
        return report_id in {
            "research_governance_summary",
            "artifact_lineage_graph",
            "artifact_lineage_validation",
        }
    return False


def _report_index_warning_row(warning: Mapping[str, Any]) -> dict[str, Any]:
    classification = _text(warning.get("warning_classification"))
    is_legacy = classification == "legacy_warning"
    metadata_only = classification == "missing_metadata"
    required_daily = bool(warning.get("required_for_daily_reading"))
    repair_type = "metadata_repair" if metadata_only else "data_regeneration"
    if classification == "governance_warning":
        repair_type = "owner_review"
    if is_legacy:
        repair_type = "owner_review"
    return {
        "warning_id": _text(warning.get("issue_id")),
        "source_report": "report_index_warning_triage",
        "report_id": _text(warning.get("report_id")),
        "affected_artifact_family": _text(warning.get("group")),
        "latest_artifact_path": _text(warning.get("latest_artifact_path")),
        "warning_scope": (
            "legacy-only"
            if is_legacy
            else ("metadata-only" if metadata_only else "report-index")
        ),
        "metadata_only": metadata_only,
        "legacy_only": is_legacy,
        "data_quality_related": classification == "stale_artifact_warning",
        "report_index_related": not is_legacy and not metadata_only,
        "owner_review_related": classification in {"true_blocker", "governance_warning"},
        "waivable": is_legacy and not required_daily,
        "needs_code_repair": False,
        "needs_metadata_repair": metadata_only,
        "needs_data_regeneration": not metadata_only and not is_legacy,
        "needs_owner_review": classification in {"true_blocker", "governance_warning"} or is_legacy,
        "repair_type": repair_type,
        "exact_next_action_required": _text(
            warning.get("owner_action"),
            _text(warning.get("proposed_action"), "review_report_index_warning"),
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _summary(
    *,
    post_recovery_pack: Mapping[str, Any],
    blocker_inventory: Sequence[Mapping[str, Any]],
    warning_inventory: Sequence[Mapping[str, Any]],
    report_index_warning_inventory: Sequence[Mapping[str, Any]],
    normal_observation_clock: Mapping[str, Any],
    owner_action_map: Mapping[str, Any],
) -> dict[str, Any]:
    post_summary = _mapping(post_recovery_pack.get("summary"))
    clock_summary = _mapping(normal_observation_clock.get("summary"))
    owner_summary = _mapping(owner_action_map.get("summary"))
    blocker_kind_counts = Counter(_text(row.get("blocker_kind")) for row in blocker_inventory)
    warning_scope_counts = Counter(_text(row.get("warning_scope")) for row in warning_inventory)
    report_index_scope_counts = Counter(
        _text(row.get("warning_scope")) for row in report_index_warning_inventory
    )
    return {
        "post_recovery_status": _text(post_recovery_pack.get("post_recovery_status")),
        "blocker_count": len(blocker_inventory),
        "warning_count": len(warning_inventory),
        "report_index_warning_count": len(report_index_warning_inventory),
        "source_post_recovery_blocker_count": _int(
            post_summary.get("remaining_blocker_count"),
            len(blocker_inventory),
        ),
        "source_post_recovery_warning_count": _int(
            post_summary.get("remaining_warning_count"),
            len(warning_inventory),
        ),
        "blocker_kind_counts": dict(blocker_kind_counts),
        "warning_scope_counts": dict(warning_scope_counts),
        "report_index_warning_scope_counts": dict(report_index_scope_counts),
        "normal_paper_shadow_may_resume": bool(
            post_summary.get("normal_paper_shadow_may_resume")
        ),
        "normal_observation_clock_status": _text(
            clock_summary.get("normal_observation_clock_status"),
            _text(post_summary.get("normal_observation_clock_status")),
        ),
        "extended_shadow_remains_forbidden": bool(
            post_summary.get("extended_shadow_remains_forbidden")
        ),
        "live_trading_remains_forbidden": bool(
            post_summary.get("live_trading_remains_forbidden")
        ),
        "latest_owner_action": _text(post_summary.get("latest_owner_action")),
        "next_owner_action": _text(
            owner_summary.get("next_owner_action"),
            _text(post_summary.get("next_owner_action")),
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _reader_brief(
    status: str,
    summary: Mapping[str, Any],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    report_index_warnings: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "summary": (
            f"Exact recovery inventory: blockers={len(blockers)}, "
            f"warnings={len(warnings)}, report-index warnings={len(report_index_warnings)}; "
            "normal paper-shadow remains blocked and extended/live remain forbidden."
        ),
        "key_result": status,
        "blocking_issues": _issue_list(blockers, "blocker_id", "exact_next_action_required"),
        "warnings": _issue_list(warnings, "warning_id", "warning_scope"),
        "report_index_warnings": _issue_list(
            report_index_warnings,
            "warning_id",
            "repair_type",
        ),
        "safety_boundary": (
            "Inventory only; no regeneration, no waiver, no owner decision write, "
            "no broker/order, production_effect=none."
        ),
        "next_action": _text(
            summary.get("next_owner_action"),
            "review_exact_blocker_warning_inventory",
        ),
        "production_effect": PRODUCTION_EFFECT,
    }


def _repair_type_for_warning(scope: str) -> str:
    if scope == "metadata-only":
        return "metadata_repair"
    if scope == "owner-review":
        return "owner_review"
    if scope in {"data-quality", "report-index"}:
        return "data_regeneration"
    return "owner_review"


def _blocker_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "blocker_id",
        "source_report",
        "source_artifact_id",
        "source_artifact_path",
        "upstream_artifact_dependency",
        "blocker_kind",
        "exact_next_action_required",
        "blocks_normal_paper_shadow",
        "blocks_extended_shadow",
        "blocks_live_trading",
    )
    return all(key in row and row.get(key) not in (None, "") for key in required)


def _warning_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "warning_id",
        "source_report",
        "warning_scope",
        "waivable",
        "needs_code_repair",
        "needs_metadata_repair",
        "needs_data_regeneration",
        "needs_owner_review",
        "repair_type",
        "exact_next_action_required",
    )
    return all(key in row and row.get(key) is not None for key in required)


def _report_index_warning_row_complete(row: Mapping[str, Any]) -> bool:
    required = (
        "warning_id",
        "report_id",
        "affected_artifact_family",
        "warning_scope",
        "repair_type",
        "waivable",
        "exact_next_action_required",
    )
    return all(key in row and row.get(key) not in (None, "") for key in required)


def _append_check(
    checks: list[dict[str, Any]],
    blocking_issues: list[dict[str, Any]],
    check_id: str,
    passed: bool,
    message: str,
    recommended_action: str,
) -> None:
    checks.append(
        {
            "check_id": check_id,
            "status": PASS_STATUS if passed else FAIL_STATUS,
            "message": message,
            "recommended_action": recommended_action,
        }
    )
    if not passed:
        blocking_issues.append(
            {
                "issue_id": check_id,
                "message": message,
                "recommended_action": recommended_action,
            }
        )


def _reader_brief_complete(value: Any) -> bool:
    reader_brief = _mapping(value)
    return all(bool(_text(reader_brief.get(field))) for field in CORE_READER_BRIEF_FIELDS)


def _safety_boundary_valid(value: Any) -> bool:
    safety = _mapping(value)
    return (
        _text(safety.get("production_effect")) == PRODUCTION_EFFECT
        and safety.get("does_not_run_upstream_commands") is True
        and safety.get("does_not_refresh_data") is True
        and safety.get("does_not_generate_missing_artifacts") is True
        and safety.get("does_not_create_waivers") is True
        and safety.get("does_not_write_owner_decision") is True
        and safety.get("candidate_state_mutated") is False
        and safety.get("paper_shadow_state_mutated") is False
        and safety.get("production_state_mutated") is False
        and safety.get("official_target_weights_generated") is False
        and safety.get("broker_action_taken") is False
        and safety.get("order_ticket_generated") is False
        and safety.get("live_trading_allowed") is False
    )


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_recovery_artifacts_only",
        "production_effect": PRODUCTION_EFFECT,
        "manual_review_only": True,
        "inventory_only": True,
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_create_waivers": True,
        "does_not_write_owner_decision": True,
        "candidate_state_mutated": False,
        "paper_shadow_state_mutated": False,
        "production_state_mutated": False,
        "official_target_weights_generated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "extended_shadow_approved": False,
        "live_trading_allowed": False,
    }


def _issue_list(
    records: Sequence[Mapping[str, Any]],
    key_field: str,
    value_field: str,
) -> str:
    if not records:
        return "none"
    return "; ".join(
        f"{_text(record.get(key_field))}:{_text(record.get(value_field))}"
        for record in records[:5]
    )


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _write_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return output_path


def _write_text(text: str, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    return output_path
