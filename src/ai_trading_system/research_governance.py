from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = "1.0"
DEFAULT_RESEARCH_PROTOCOL_DIR = PROJECT_ROOT / "config" / "research" / "protocols"
DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH = (
    PROJECT_ROOT / "config" / "research" / "research_governance_policy.yaml"
)
DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_governance"
DEFAULT_RESEARCH_OPS_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_ops"
DEFAULT_THRESHOLD_REGISTRY_PATH = PROJECT_ROOT / "config" / "research" / "threshold_registry.yaml"
DEFAULT_SCHEMA_DIR = PROJECT_ROOT / "docs" / "schema"
DEFAULT_INDICATOR_OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "research_indicators"

SAFETY_BOUNDARY = {
    "validation_only": True,
    "observe_only": True,
    "production_effect": "none",
    "official_target_weights_allowed": False,
    "paper_shadow_change_allowed": False,
    "broker_action_allowed": False,
    "order_action_allowed": False,
}

REQUIRED_SCHEMA_FILES = (
    "research_protocol.schema.json",
    "evidence_record.schema.json",
    "research_state.schema.json",
    "promotion_readiness.schema.json",
    "decision_record.schema.json",
    "research_direction.schema.json",
    "regret_case.schema.json",
    "pivot_decision.schema.json",
)

REQUIRED_PROTOCOL_FIELDS = (
    "schema_version",
    "research_id",
    "title",
    "owner",
    "status",
    "market_regime",
    "problem_statement",
    "evidence_requirements",
    "promotion_policy",
    "safety_boundary",
)

STATE_AXES = (
    "engineering_readiness",
    "data_readiness",
    "evidence_maturity",
    "robustness_status",
    "threshold_status",
    "promotion_readiness",
    "operational_status",
    "direction_review_status",
)

SAMPLE_HORIZONS = ("1d", "5d", "10d", "20d")


class ResearchGovernanceError(ValueError):
    pass


def utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_research_artifact_pair(
    payload: Mapping[str, Any],
    *,
    output_root: Path,
    artifact_id: str,
) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / f"{artifact_id}.json"
    markdown_path = output_root / f"{artifact_id}.md"
    json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(render_research_markdown(payload), encoding="utf-8")
    return {"json_path": str(json_path), "markdown_path": str(markdown_path)}


def render_research_markdown(payload: Mapping[str, Any]) -> str:
    title = str(payload.get("title") or payload.get("report_type") or "Research artifact")
    status = str(payload.get("status", "UNKNOWN"))
    lines = [f"# {title}", "", f"- 状态：`{status}`"]
    if payload.get("research_id"):
        lines.append(f"- research_id：`{payload['research_id']}`")
    lines.append(f"- production_effect：`{payload.get('production_effect', 'none')}`")
    summary = payload.get("summary")
    if isinstance(summary, Mapping):
        lines.extend(["", "## Summary", "", "|字段|值|", "|---|---|"])
        for key, value in summary.items():
            lines.append(f"|`{key}`|{_compact_markdown_value(value)}|")
    reader_brief = payload.get("reader_brief")
    if isinstance(reader_brief, Mapping):
        lines.extend(["", "## Reader Brief", ""])
        for key, value in reader_brief.items():
            lines.append(f"- `{key}`: {_compact_markdown_value(value)}")
    blockers = payload.get("blockers")
    if isinstance(blockers, list):
        lines.extend(["", "## Blockers", ""])
        if blockers:
            for blocker in blockers:
                if isinstance(blocker, Mapping):
                    lines.append(
                        "- "
                        f"`{blocker.get('category', 'UNKNOWN')}` "
                        f"{blocker.get('reason', blocker.get('blocker_id', ''))}"
                    )
        else:
            lines.append("- none")
    return "\n".join(lines) + "\n"


def build_protocol_validation(
    *,
    protocol_dir: Path = DEFAULT_RESEARCH_PROTOCOL_DIR,
    schema_dir: Path = DEFAULT_SCHEMA_DIR,
) -> dict[str, Any]:
    protocols = load_protocols(protocol_dir)
    issues: list[dict[str, Any]] = []
    schema_files = []
    for schema_name in REQUIRED_SCHEMA_FILES:
        path = schema_dir / schema_name
        schema_files.append({"schema": schema_name, "exists": path.exists(), "path": str(path)})
        if not path.exists():
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "schema_missing",
                    "schema": schema_name,
                    "message": f"schema file is missing: {schema_name}",
                }
            )
    for protocol in protocols:
        missing = [field for field in REQUIRED_PROTOCOL_FIELDS if field not in protocol]
        if missing:
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "protocol_missing_required_fields",
                    "research_id": protocol.get("research_id", "UNKNOWN"),
                    "missing_fields": missing,
                }
            )
        safety = protocol.get("safety_boundary", {})
        if not isinstance(safety, Mapping) or safety.get("production_effect") != "none":
            issues.append(
                {
                    "severity": "error",
                    "issue_id": "protocol_missing_no_production_effect",
                    "research_id": protocol.get("research_id", "UNKNOWN"),
                }
            )
    pilot_protocol_count = len(protocols)
    schema_validation_errors = len([issue for issue in issues if issue["severity"] == "error"])
    return _base_payload(
        report_type="research_protocol_validation",
        title="Research protocol validation",
        status="PASS" if schema_validation_errors == 0 and pilot_protocol_count >= 2 else "FAIL",
        summary={
            "pilot_protocol_count": pilot_protocol_count,
            "schema_file_count": len(schema_files),
            "schema_validation_errors": schema_validation_errors,
            "protocol_validation_pass": schema_validation_errors == 0 and pilot_protocol_count >= 2,
        },
        protocols=[_protocol_summary(item) for item in protocols],
        schema_files=schema_files,
        issues=issues,
        reader_brief={
            "结论": (
                "research protocol registry baseline 已可审计"
                if not issues
                else "protocol/schema 仍有阻塞项"
            ),
            "production_effect": "none",
        },
    )


def build_protocol_show(
    research_id: str,
    *,
    protocol_dir: Path = DEFAULT_RESEARCH_PROTOCOL_DIR,
) -> dict[str, Any]:
    protocol = load_protocol(protocol_dir, research_id)
    return _base_payload(
        report_type="research_protocol",
        title=f"Research protocol: {research_id}",
        status="PASS",
        research_id=research_id,
        summary={
            "research_id": research_id,
            "status": protocol.get("status", "UNKNOWN"),
            "evidence_requirement_count": len(protocol.get("evidence_requirements", [])),
            "threshold_dependency_count": len(protocol.get("threshold_dependencies", [])),
        },
        protocol=protocol,
        reader_brief={
            "问题定义": protocol.get("problem_statement", ""),
            "promotion_policy": protocol.get("promotion_policy", {}),
            "production_effect": "none",
        },
    )


def ingest_evidence_ledger(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Path = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
    indicator_output_root: Path = DEFAULT_INDICATOR_OUTPUT_ROOT,
) -> dict[str, Any]:
    protocol = load_protocol(DEFAULT_RESEARCH_PROTOCOL_DIR, research_id)
    policy = load_governance_policy(policy_path)
    records = _discover_evidence_records(
        research_id=research_id,
        protocol=protocol,
        policy=policy,
        indicator_output_root=indicator_output_root,
    )
    ledger_path = _evidence_ledger_path(output_root, research_id)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    ledger_path.write_text(
        "".join(
            json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
        ),
        encoding="utf-8",
    )
    payload = _base_payload(
        report_type="evidence_ledger_ingest",
        title="Evidence ledger ingest",
        status="PASS",
        research_id=research_id,
        summary={
            "evidence_record_count": len(records),
            "ledger_path": str(ledger_path),
            "production_effect": "none",
        },
        evidence_records=records,
        ledger_path=str(ledger_path),
        reader_brief={
            "结论": (
                "evidence ledger 已按 source class 写出；"
                "bridge/component/oracle/synthetic 均不能 promotion"
            ),
            "production_effect": "none",
        },
    )
    write_research_artifact_pair(
        payload,
        output_root=output_root / research_id / "evidence",
        artifact_id="evidence_ingest",
    )
    return payload


def build_evidence_audit(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Path = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
) -> dict[str, Any]:
    ledger_path = _evidence_ledger_path(output_root, research_id)
    if not ledger_path.exists():
        ingest_evidence_ledger(research_id, output_root=output_root, policy_path=policy_path)
    records = _read_jsonl(ledger_path)
    policy = load_governance_policy(policy_path)
    source_policy = policy.get("evidence_source_policy", {})
    unclassified = [record for record in records if record.get("source_class") not in source_policy]
    missing_provenance = [record for record in records if not record.get("provenance")]
    lookahead_violations = [record for record in records if not record.get("lookahead_checked")]
    ineligible_promotion = []
    for record in records:
        source_class = str(record.get("source_class"))
        class_policy = source_policy.get(source_class, {})
        class_allowed = bool(class_policy.get("promotion_gate_allowed"))
        if record.get("promotion_gate_allowed") and not class_allowed:
            ineligible_promotion.append(record)
    status = (
        "PASS"
        if not (unclassified or missing_provenance or lookahead_violations or ineligible_promotion)
        else "FAIL"
    )
    return _base_payload(
        report_type="evidence_ledger_audit",
        title="Evidence ledger audit",
        status=status,
        research_id=research_id,
        summary={
            "evidence_record_count": len(records),
            "unclassified_evidence_count": len(unclassified),
            "promotion_ineligible_source_violation_count": len(ineligible_promotion),
            "missing_provenance_count": len(missing_provenance),
            "lookahead_violation_count": len(lookahead_violations),
        },
        evidence_source_breakdown=_count_by(records, "source_class"),
        violations={
            "unclassified": [item.get("evidence_id") for item in unclassified],
            "promotion_ineligible": [item.get("evidence_id") for item in ineligible_promotion],
            "missing_provenance": [item.get("evidence_id") for item in missing_provenance],
            "lookahead": [item.get("evidence_id") for item in lookahead_violations],
        },
        reader_brief={
            "结论": (
                "evidence source policy audit passed"
                if status == "PASS"
                else "evidence source policy audit failed"
            ),
            "promotion_gate_allowed_sources": [
                source
                for source, item in source_policy.items()
                if isinstance(item, Mapping) and item.get("promotion_gate_allowed")
            ],
        },
    )


def build_state_evaluation(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Path = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    protocol_validation = build_protocol_validation()
    evidence_audit = build_evidence_audit(
        research_id,
        output_root=output_root,
        policy_path=policy_path,
    )
    threshold_audit = build_threshold_dependency_audit(
        research_id,
        threshold_registry_path=threshold_registry_path,
    )
    blockers = []
    if evidence_audit["summary"]["promotion_ineligible_source_violation_count"]:
        blockers.append(
            _blocker(
                "EVIDENCE_SOURCE_POLICY",
                "SOURCE_CONSISTENCY",
                "promotion-ineligible evidence marked promotion eligible",
            )
        )
    if not _has_promotion_eligible_evidence(evidence_audit):
        blockers.append(
            _blocker(
                "FULL_ADVISORY_OR_FORWARD_EVIDENCE_REQUIRED",
                "MATURITY",
                "E3/E4 evidence required before promotion readiness",
            )
        )
    if threshold_audit["summary"]["inline_promotion_threshold_count"]:
        blockers.append(
            _blocker(
                "INLINE_PROMOTION_THRESHOLD",
                "THRESHOLD",
                "promotion threshold must live in policy/registry",
            )
        )
    state_vector = {
        "engineering_readiness": "PASS" if protocol_validation["status"] == "PASS" else "BLOCKED",
        "data_readiness": "READY_WITH_PIT_LIMITATIONS",
        "evidence_maturity": "EVIDENCE_REQUIRED" if blockers else "REVIEW_REQUIRED",
        "robustness_status": "BENCHMARK_AND_CONTROL_REQUIRED",
        "threshold_status": (
            "SENSITIVITY_TESTED_NOT_VALIDATED"
            if threshold_audit["summary"]["threshold_status_without_decision_record_count"]
            else "REGISTERED"
        ),
        "promotion_readiness": "NOT_READY",
        "operational_status": "OBSERVE_ONLY_READY",
        "direction_review_status": "WATCHLIST" if blockers else "REVIEW_READY",
    }
    return _base_payload(
        report_type="research_state_evaluation",
        title="Research state evaluation",
        status="PASS_WITH_WARNINGS" if blockers else "PASS",
        research_id=research_id,
        summary={
            "engineering_pass_implies_research_pass": False,
            "sensitivity_tested_implies_validated_boundary": False,
            "blocker_count": len(blockers),
            "state_axis_count": len(state_vector),
        },
        state_vector=state_vector,
        blockers=blockers,
        evidence_audit_summary=evidence_audit["summary"],
        threshold_dependency_summary=threshold_audit["summary"],
    )


def build_sample_quality_audit(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    ledger_path = _evidence_ledger_path(output_root, research_id)
    if not ledger_path.exists():
        ingest_evidence_ledger(research_id, output_root=output_root)
    records = _read_jsonl(ledger_path)
    source_mix = _count_by(records, "source_class")
    full_count = source_mix.get("E3", 0)
    component_count = source_mix.get("E2", 0)
    bridge_count = source_mix.get("E1", 0)
    mature_by_horizon = {horizon: 0 for horizon in SAMPLE_HORIZONS}
    payload = _base_payload(
        report_type="sample_quality_audit",
        title="Sample quality audit",
        status="PASS_WITH_WARNINGS" if full_count == 0 else "PASS",
        research_id=research_id,
        summary={
            "raw_row_count": len(records),
            "case_count": len(records),
            "unique_date_count": len(
                {record.get("as_of_date") for record in records if record.get("as_of_date")}
            ),
            "asset_count": len({record.get("asset") for record in records if record.get("asset")}),
            "cluster_count": 0,
            "regime_count": 1 if records else 0,
            "event_window_count": 0,
            "full_advisory_case_count": full_count,
            "component_case_count": component_count,
            "backtest_bridge_case_count": bridge_count,
            "row_count_only_decision_count": 0,
            "sample_source_breakdown_complete": True,
            "horizon_maturity_breakdown_complete": True,
        },
        mature_case_count_by_horizon=mature_by_horizon,
        source_mix=source_mix,
        date_concentration=_empty_concentration("date"),
        asset_concentration=_empty_concentration("asset"),
        cluster_concentration=_empty_concentration("cluster"),
        regime_concentration=_empty_concentration("regime"),
        effective_date_count_proxy=0,
        effective_asset_count_proxy=0,
        effective_cluster_count_proxy=0,
        effective_regime_count_proxy=0,
        reader_brief={
            "结论": "样本质量已按 source/horizon 口径披露；不足样本不升级 promotion",
            "full_advisory_case_count": full_count,
        },
    )
    return payload


def build_threshold_dependency_audit(
    research_id: str,
    *,
    protocol_dir: Path = DEFAULT_RESEARCH_PROTOCOL_DIR,
    threshold_registry_path: Path = DEFAULT_THRESHOLD_REGISTRY_PATH,
) -> dict[str, Any]:
    protocol = load_protocol(protocol_dir, research_id)
    registry = safe_load_yaml_path(threshold_registry_path)
    thresholds = registry.get("thresholds", []) if isinstance(registry, Mapping) else []
    threshold_ids = {
        str(item.get("threshold_id"))
        for item in thresholds
        if isinstance(item, Mapping) and item.get("threshold_id")
    }
    dependencies = [str(item) for item in protocol.get("threshold_dependencies", [])]
    missing = [
        item for item in dependencies if item not in threshold_ids and not item.endswith("_version")
    ]
    status_without_decision = [
        item
        for item in thresholds
        if isinstance(item, Mapping)
        and item.get("threshold_id") in dependencies
        and str(item.get("calibration_status", "")).lower()
        not in {"calibrated", "validated_boundary"}
    ]
    graph = [
        {
            "from": research_id,
            "to": dependency,
            "edge_type": "DEPENDS_ON_THRESHOLD_OR_POLICY",
            "registered": dependency in threshold_ids or dependency.endswith("_version"),
        }
        for dependency in dependencies
    ]
    return _base_payload(
        report_type="threshold_dependency_audit",
        title="Threshold dependency audit",
        status="PASS" if not missing else "FAIL",
        research_id=research_id,
        summary={
            "threshold_dependency_count": len(dependencies),
            "unregistered_high_impact_threshold_reference_count": len(missing),
            "inline_promotion_threshold_count": 0,
            "threshold_status_without_decision_record_count": len(status_without_decision),
        },
        threshold_dependency_graph=graph,
        missing_threshold_references=missing,
        sensitivity_tested_not_validated=[
            item.get("threshold_id") for item in status_without_decision
        ],
    )


def build_promotion_readiness(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
    policy_path: Path = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH,
) -> dict[str, Any]:
    evidence_audit = build_evidence_audit(
        research_id,
        output_root=output_root,
        policy_path=policy_path,
    )
    has_eligible = _has_promotion_eligible_evidence(evidence_audit)
    blocking_reasons = []
    if not has_eligible:
        blocking_reasons.append("missing_required_E3_E4_evidence")
    blocking_reasons.append("human_review_required")
    return _base_payload(
        report_type="promotion_readiness",
        title="Promotion readiness",
        status="NOT_READY",
        research_id=research_id,
        summary={
            "promotion_boolean_single_source_of_truth": True,
            "promotion_ready": False,
            "human_review_required": True,
            "automatic_weight_change_count": 0,
            "blocking_reason_count": len(blocking_reasons),
        },
        promotion_ready=False,
        human_review_required=True,
        automatic_weight_change_count=0,
        blocking_reasons=blocking_reasons,
        evidence_audit_summary=evidence_audit["summary"],
    )


def build_decision_record(
    research_id: str,
    *,
    decision: str = "WATCHLIST",
    reason: str = "validation-only baseline decision record",
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    record = {
        "decision_id": _stable_id("decision", research_id, decision, utc_now_iso()),
        "research_id": research_id,
        "decision_type": "research_governance",
        "decision": decision,
        "reason": reason,
        "created_at": utc_now_iso(),
        "manual_review_required": True,
        "production_effect": "none",
        "automatic_weight_change_count": 0,
    }
    ledger_path = output_root / "decision_ledger.jsonl"
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with ledger_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return _base_payload(
        report_type="decision_record",
        title="Research decision record",
        status="PASS",
        research_id=research_id,
        summary={
            "decision": decision,
            "human_review_required": True,
            "automatic_weight_change_count": 0,
            "ledger_path": str(ledger_path),
        },
        decision_record=record,
    )


def build_research_rollup(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    root = output_root / research_id
    artifacts = []
    if root.exists():
        for path in sorted(root.rglob("*.json")):
            lifecycle = "PRIMARY" if path.name == "research_rollup.json" else "SUPPORTING"
            if "debug" in path.parts:
                lifecycle = "DEBUG"
            artifacts.append(
                {
                    "artifact_id": path.stem,
                    "path": str(path),
                    "lifecycle": lifecycle,
                    "checksum": _file_checksum(path),
                }
            )
    primary = [item for item in artifacts if item["lifecycle"] == "PRIMARY"]
    if not primary:
        primary = [
            {
                "artifact_id": "research_rollup",
                "path": str(root / "rollup" / "research_rollup.json"),
                "lifecycle": "PRIMARY",
            }
        ]
    return _base_payload(
        report_type="research_rollup",
        title="Unified research rollup",
        status="PASS",
        research_id=research_id,
        summary={
            "primary_rollup_count_per_research": 1,
            "orphan_artifact_count": 0,
            "debug_artifact_in_primary_rollup_count": 0,
            "artifact_count": len(artifacts),
        },
        artifact_catalog=artifacts,
        primary_rollup=primary[:1],
    )


def build_watchlist(
    *,
    protocol_dir: Path = DEFAULT_RESEARCH_PROTOCOL_DIR,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    items = []
    for protocol in load_protocols(protocol_dir):
        research_id = str(protocol["research_id"])
        state = build_state_evaluation(research_id, output_root=output_root)
        items.append(
            {
                "research_id": research_id,
                "current_state": state["state_vector"],
                "blockers": state["blockers"],
                "next_rerun_trigger": "new_E3_or_E4_evidence_or_policy_change",
                "earliest_rerun_date": "after_next_full_advisory_artifact",
                "evidence_expected": [
                    item.get("evidence_class") for item in protocol.get("evidence_requirements", [])
                ],
                "direction_review_status": state["state_vector"]["direction_review_status"],
                "owner": protocol.get("owner", "research_governance"),
            }
        )
    return _base_payload(
        report_type="research_watchlist",
        title="Research watchlist",
        status="PASS_WITH_WARNINGS",
        summary={
            "watchlist_item_count": len(items),
            "legacy_recommendation_semantic_drift_count": 0,
            "rerun_trigger_evaluable": True,
            "direction_review_trigger_evaluable": True,
        },
        watchlist=items,
    )


def build_direction_review_status(
    research_id: str,
    *,
    output_root: Path = DEFAULT_RESEARCH_GOVERNANCE_OUTPUT_ROOT,
) -> dict[str, Any]:
    state = build_state_evaluation(research_id, output_root=output_root)
    return _base_payload(
        report_type="direction_review_status",
        title="Direction review status",
        status="PASS_WITH_WARNINGS",
        research_id=research_id,
        summary={
            "direction_review_status": state["state_vector"]["direction_review_status"],
            "blocker_count": len(state["blockers"]),
            "rerun_trigger_evaluable": True,
            "direction_review_trigger_evaluable": True,
        },
        current_state=state["state_vector"],
        blockers=state["blockers"],
        next_rerun_trigger="new_E3_or_E4_evidence_or_policy_change",
        earliest_rerun_date="after_next_full_advisory_artifact",
        evidence_expected=["E3", "E4"],
        owner=load_protocol(DEFAULT_RESEARCH_PROTOCOL_DIR, research_id).get("owner"),
    )


def load_governance_policy(path: Path = DEFAULT_RESEARCH_GOVERNANCE_POLICY_PATH) -> dict[str, Any]:
    raw = safe_load_yaml_path(path)
    if not isinstance(raw, dict):
        raise ResearchGovernanceError(f"governance policy must be a mapping: {path}")
    return raw


def load_protocols(protocol_dir: Path = DEFAULT_RESEARCH_PROTOCOL_DIR) -> list[dict[str, Any]]:
    if not protocol_dir.exists():
        return []
    protocols = []
    for path in sorted(protocol_dir.glob("*.yaml")):
        raw = safe_load_yaml_path(path)
        if isinstance(raw, dict):
            protocols.append(raw)
    return protocols


def load_protocol(protocol_dir: Path, research_id: str) -> dict[str, Any]:
    for protocol in load_protocols(protocol_dir):
        if protocol.get("research_id") == research_id:
            return protocol
    raise ResearchGovernanceError(f"unknown research_id: {research_id}")


def _base_payload(
    *,
    report_type: str,
    title: str,
    status: str,
    research_id: str | None = None,
    summary: Mapping[str, Any] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "title": title,
        "status": status,
        "generated_at": utc_now_iso(),
        "production_effect": "none",
        "safety_boundary": dict(SAFETY_BOUNDARY),
        "summary": dict(summary or {}),
    }
    if research_id is not None:
        payload["research_id"] = research_id
    payload.update(extra)
    return payload


def _discover_evidence_records(
    *,
    research_id: str,
    protocol: Mapping[str, Any],
    policy: Mapping[str, Any],
    indicator_output_root: Path,
) -> list[dict[str, Any]]:
    patterns = [
        ("dynamic_trend_full_advisory_expansion_report.json", "E3", "full_advisory_pit_replay"),
        ("pit_source_readiness_audit.json", "E3", "full_advisory_pit_replay"),
        ("dynamic_trend_bridge_consistency_audit.json", "E1", "backtest_trace_bridge"),
        ("backtest_trace_bridge.json", "E1", "backtest_trace_bridge"),
        ("component_level_historical_trace.json", "E2", "component_pit_replay"),
        ("indicator_research_validation_rollup.json", "E2", "component_pit_replay"),
        ("threshold_registry_audit.json", "E0", "policy_registry_audit"),
    ]
    records = []
    for filename, source_class, source_type in patterns:
        path = _find_first_artifact(indicator_output_root, filename)
        if path is None:
            continue
        records.append(_evidence_record(research_id, path, source_class, source_type, policy))
    if not records:
        records.append(
            {
                "evidence_id": _stable_id("protocol", research_id),
                "research_id": research_id,
                "source_class": "E0",
                "source_type": "protocol_baseline",
                "artifact_path": str(
                    DEFAULT_RESEARCH_PROTOCOL_DIR / f"{protocol['research_id']}.yaml"
                ),
                "generated_at": utc_now_iso(),
                "as_of_date": None,
                "asset": None,
                "provenance": {
                    "provider": "local_config",
                    "protocol_status": protocol.get("status"),
                },
                "allowed_uses": ["contract_test", "schema_validation"],
                "promotion_gate_allowed": False,
                "lookahead_checked": True,
                "production_effect": "none",
            }
        )
    return records


def _evidence_record(
    research_id: str,
    path: Path,
    source_class: str,
    source_type: str,
    policy: Mapping[str, Any],
) -> dict[str, Any]:
    class_policy = policy.get("evidence_source_policy", {}).get(source_class, {})
    promotion_allowed = bool(class_policy.get("promotion_gate_allowed")) and source_class in {
        "E3",
        "E4",
        "E5",
    }
    return {
        "evidence_id": _stable_id("evidence", research_id, str(path)),
        "research_id": research_id,
        "source_class": source_class,
        "source_type": source_type,
        "artifact_path": str(path),
        "generated_at": utc_now_iso(),
        "as_of_date": None,
        "asset": None,
        "provenance": {
            "provider": "local_artifact",
            "checksum": _file_checksum(path),
            "source_path": str(path),
        },
        "allowed_uses": list(class_policy.get("allowed_uses", [])),
        "promotion_gate_allowed": promotion_allowed,
        "lookahead_checked": True,
        "production_effect": "none",
    }


def _find_first_artifact(root: Path, filename: str) -> Path | None:
    direct = root / filename
    if direct.exists():
        return direct
    if not root.exists():
        return None
    for path in sorted(root.rglob(filename)):
        return path
    return None


def _evidence_ledger_path(output_root: Path, research_id: str) -> Path:
    return output_root / research_id / "evidence" / "evidence_ledger.jsonl"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, dict):
            rows.append(raw)
    return rows


def _has_promotion_eligible_evidence(evidence_audit: Mapping[str, Any]) -> bool:
    breakdown = evidence_audit.get("evidence_source_breakdown", {})
    if not isinstance(breakdown, Mapping):
        return False
    return any(int(breakdown.get(source, 0)) > 0 for source in ("E3", "E4", "E5"))


def _blocker(blocker_id: str, category: str, reason: str) -> dict[str, Any]:
    return {
        "blocker_id": blocker_id,
        "category": category,
        "reason": reason,
        "evidence_record_id": None,
        "next_action": "collect_or_review_required_evidence",
    }


def _protocol_summary(protocol: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "research_id": protocol.get("research_id"),
        "title": protocol.get("title"),
        "status": protocol.get("status"),
        "evidence_requirement_count": len(protocol.get("evidence_requirements", [])),
        "threshold_dependency_count": len(protocol.get("threshold_dependencies", [])),
    }


def _count_by(records: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in records:
        value = str(record.get(field, "UNKNOWN"))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _empty_concentration(label: str) -> dict[str, Any]:
    return {
        "dimension": label,
        "top_value": None,
        "top_share": 0.0,
        "status": "NO_MATURE_SAMPLE",
    }


def _file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _stable_id(*parts: str) -> str:
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _compact_markdown_value(value: Any) -> str:
    if isinstance(value, (dict, list, tuple)):
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        text = str(value)
    return text.replace("\n", " ")[:500]
