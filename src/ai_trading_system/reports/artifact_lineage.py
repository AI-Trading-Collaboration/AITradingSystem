from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.core import ArtifactRef
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_INDEX_WAIVER_PATH,
    DEFAULT_REPORT_REGISTRY_PATH,
    build_report_index_payload,
)

SCHEMA_VERSION = 1
REPORT_TYPE = "artifact_lineage_graph"
VALIDATION_REPORT_TYPE = "artifact_lineage_validation"
PRODUCTION_EFFECT = "none"
PASS_STATUS = "PASS"
WARN_STATUS = "PASS_WITH_WARNINGS"
FAIL_STATUS = "FAIL"


@dataclass(frozen=True)
class FamilySpec:
    family_id: str
    label: str
    report_ids: tuple[str, ...]
    path_patterns: tuple[str, ...] = ()


@dataclass(frozen=True)
class EdgeSpec:
    from_family: str
    to_family: str
    relationship: str


FAMILY_SPECS: tuple[FamilySpec, ...] = (
    FamilySpec(
        "data_artifacts",
        "Data artifacts",
        ("data_quality", "market_data_freshness", "market_data_refresh"),
        (
            "data/raw/prices_daily.csv",
            "data/raw/prices_marketstack_daily.csv",
            "data/raw/rates_daily.csv",
            "data/processed/features_daily.csv",
            "data/processed/scores_daily.csv",
        ),
    ),
    FamilySpec("cache_catalog", "Cache catalog", ("cache_catalog",)),
    FamilySpec("refresh_audit", "Refresh audit", ("data_refresh_audit",)),
    FamilySpec(
        "pit_manifest",
        "PIT manifest",
        ("pit_source_manifest",),
        ("data/raw/pit_snapshots/manifest.csv",),
    ),
    FamilySpec(
        "signal_artifacts",
        "Signal artifacts",
        (
            "etf_dynamic_v3_signal_input_completeness",
            "signal_snapshot",
            "signal_ablation",
            "signal_calibration",
        ),
    ),
    FamilySpec(
        "daily_paper_shadow",
        "Daily paper-shadow artifacts",
        ("etf_dynamic_v3_paper_shadow_daily", "etf_dynamic_v3_paper_shadow"),
    ),
    FamilySpec(
        "drift_monitor",
        "Drift monitor artifacts",
        ("etf_dynamic_v3_paper_shadow_drift_monitor",),
    ),
    FamilySpec(
        "weekly_review",
        "Weekly review artifacts",
        ("etf_dynamic_v3_paper_shadow_weekly_review",),
    ),
    FamilySpec(
        "staleness_monitor",
        "Staleness monitor artifacts",
        ("etf_dynamic_v3_evidence_staleness_monitor",),
    ),
    FamilySpec(
        "readiness_reports",
        "Readiness reports",
        (
            "etf_dynamic_v3_shadow_continuation_readiness",
            "etf_dynamic_v3_filtered_formalization_readiness",
            "etf_dynamic_v3_overnight_readiness",
        ),
    ),
    FamilySpec(
        "owner_reviews",
        "Owner reviews",
        (
            "etf_dynamic_v3_owner_filtered_candidate_review",
            "etf_dynamic_v3_owner_review",
            "etf_dynamic_v3_owner_attribution",
            "etf_dynamic_v3_rule_owner_decision",
            "etf_dynamic_v3_defensive_owner_pack",
        ),
    ),
)

EDGE_SPECS: tuple[EdgeSpec, ...] = (
    EdgeSpec("data_artifacts", "cache_catalog", "cache_artifacts_are_cataloged"),
    EdgeSpec("cache_catalog", "refresh_audit", "cache_catalog_links_refresh_evidence"),
    EdgeSpec("refresh_audit", "pit_manifest", "refresh_audit_feeds_pit_visibility"),
    EdgeSpec("pit_manifest", "daily_paper_shadow", "pit_context_bounds_daily_shadow"),
    EdgeSpec("signal_artifacts", "daily_paper_shadow", "signals_feed_daily_shadow"),
    EdgeSpec("daily_paper_shadow", "drift_monitor", "daily_shadow_feeds_drift_monitor"),
    EdgeSpec("daily_paper_shadow", "weekly_review", "daily_shadow_feeds_weekly_review"),
    EdgeSpec("drift_monitor", "readiness_reports", "drift_status_feeds_readiness"),
    EdgeSpec("weekly_review", "readiness_reports", "weekly_decision_feeds_readiness"),
    EdgeSpec("staleness_monitor", "readiness_reports", "freshness_status_feeds_readiness"),
    EdgeSpec("readiness_reports", "owner_reviews", "readiness_feeds_owner_review"),
)

REQUIRED_FAMILIES = tuple(spec.family_id for spec in FAMILY_SPECS)
SAFE_PRODUCTION_EFFECTS = frozenset({"", "none", "read_only", "advisory"})


def default_artifact_lineage_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lineage_graph_{as_of.isoformat()}.json"


def default_artifact_lineage_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lineage_graph_{as_of.isoformat()}.md"


def default_artifact_lineage_validation_json_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lineage_validation_{as_of.isoformat()}.json"


def default_artifact_lineage_validation_markdown_path(output_dir: Path, as_of: date) -> Path:
    return output_dir / f"artifact_lineage_validation_{as_of.isoformat()}.md"


def build_artifact_lineage_payload(
    *,
    as_of: date,
    project_root: Path = PROJECT_ROOT,
    report_index_payload: Mapping[str, Any] | None = None,
    report_index_path: Path | None = None,
    registry_path: Path = DEFAULT_REPORT_REGISTRY_PATH,
    waiver_path: Path | None = DEFAULT_REPORT_INDEX_WAIVER_PATH,
) -> dict[str, Any]:
    """Build a read-only lineage graph from existing artifact/report metadata."""
    resolved_index = (
        dict(report_index_payload)
        if isinstance(report_index_payload, Mapping)
        else build_report_index_payload(
            as_of=as_of,
            project_root=project_root,
            registry_path=registry_path,
            waiver_path=waiver_path,
        )
    )
    nodes = _lineage_nodes(
        report_index_payload=resolved_index,
        project_root=project_root,
    )
    edges = _lineage_edges(nodes)
    base_payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "as_of": as_of.isoformat(),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "production_effect": PRODUCTION_EFFECT,
        "purpose": (
            "Expose the read-only artifact dependency chain that supports candidate "
            "research and paper-shadow decisions."
        ),
        "input_artifacts": {
            "report_index": "" if report_index_path is None else str(report_index_path),
            "report_registry": str(registry_path),
            "report_index_status": _text(resolved_index.get("status"), "UNKNOWN"),
        },
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "The graph observes existing artifacts only; it does not generate missing inputs.",
            (
                "Edges are family-level audit dependencies, not proof that every row-level "
                "value is PIT-safe."
            ),
            "Missing or stale source artifacts must be repaired upstream, not in this report.",
        ],
        "required_node_families": list(REQUIRED_FAMILIES),
        "nodes": nodes,
        "edges": edges,
        "methodology": {
            "mode": "read_existing_artifacts_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
            "required_family_count": len(REQUIRED_FAMILIES),
            "required_edge_count": len(EDGE_SPECS),
        },
    }
    validation = validate_artifact_lineage_payload(base_payload)
    status = validation["status"]
    summary = _summary(nodes=nodes, edges=edges, validation=validation)
    base_payload.update(
        {
            "status": status,
            "lineage_status": status,
            "output_decision": status,
            "next_action": _next_action(status),
            "summary": summary,
            "family_coverage": _family_coverage(nodes),
            "blocking_issues": validation["blocking_issues"],
            "warning_issues": validation["warning_issues"],
            "validation_summary": validation["summary"],
        }
    )
    return base_payload


def validate_artifact_lineage_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    nodes = _records(payload.get("nodes"))
    edges = _records(payload.get("edges"))
    checks: list[dict[str, Any]] = []
    blocking_issues: list[dict[str, Any]] = []
    warning_issues: list[dict[str, Any]] = []

    _append_check(
        checks,
        blocking_issues,
        check_id="report_type",
        passed=_text(payload.get("report_type")) == REPORT_TYPE,
        severity="BLOCKING",
        message=f"report_type must be {REPORT_TYPE}.",
        recommended_action="regenerate_artifact_lineage_graph_with_supported_report_type",
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="production_effect",
        passed=_text(payload.get("production_effect")) == PRODUCTION_EFFECT,
        severity="BLOCKING",
        message="artifact lineage graph must be production_effect=none.",
        recommended_action="regenerate_lineage_graph_without_production_mutation",
    )
    node_ids = [_text(node.get("node_id")) for node in nodes]
    duplicate_node_ids = sorted(
        node_id for node_id in set(node_ids) if node_id and node_ids.count(node_id) > 1
    )
    _append_check(
        checks,
        blocking_issues,
        check_id="node_id_uniqueness",
        passed=not duplicate_node_ids,
        severity="BLOCKING",
        message="lineage node ids must be unique.",
        recommended_action="fix_duplicate_lineage_node_ids_before_reader_use",
        details={"duplicate_node_ids": duplicate_node_ids},
    )

    for family in REQUIRED_FAMILIES:
        family_nodes = [node for node in nodes if _text(node.get("family")) == family]
        available = [node for node in family_nodes if bool(node.get("exists"))]
        passed = bool(available)
        _append_check(
            checks,
            blocking_issues,
            check_id=f"required_family_{family}",
            passed=passed,
            severity="BLOCKING",
            message=f"required lineage family has no available artifact: {family}.",
            recommended_action=f"restore_or_generate_{family}_artifact_before_lineage_validation",
            family=family,
        )

    for spec in EDGE_SPECS:
        matching_edges = [
            edge
            for edge in edges
            if _text(edge.get("from_family")) == spec.from_family
            and _text(edge.get("to_family")) == spec.to_family
        ]
        passed = any(_text(edge.get("status")) == PASS_STATUS for edge in matching_edges)
        _append_check(
            checks,
            blocking_issues,
            check_id=f"required_edge_{spec.from_family}_to_{spec.to_family}",
            passed=passed,
            severity="BLOCKING",
            message=(
                "required lineage dependency edge is missing or points to missing nodes: "
                f"{spec.from_family}->{spec.to_family}."
            ),
            recommended_action="restore_upstream_and_downstream_artifacts_before_lineage_validation",
            details={"relationship": spec.relationship},
        )

    for node in nodes:
        production_effect = _text(node.get("production_effect"))
        if production_effect not in SAFE_PRODUCTION_EFFECTS:
            _append_check(
                checks,
                blocking_issues,
                check_id=f"safe_production_effect_{_text(node.get('node_id'))}",
                passed=False,
                severity="BLOCKING",
                message=(
                    f"lineage node {_text(node.get('node_id'))} has unsafe "
                    f"production_effect={production_effect}."
                ),
                recommended_action="remove_or_reclassify_artifact_before_lineage_reader_use",
                family=_text(node.get("family")),
                node_id=_text(node.get("node_id")),
            )
        freshness = _text(node.get("freshness_status")).upper()
        if bool(node.get("exists")) and freshness in {"STALE", "WARNING"}:
            _append_check(
                checks,
                warning_issues,
                check_id=f"freshness_{_text(node.get('node_id'))}",
                passed=False,
                severity="WARNING",
                message=f"lineage node {_text(node.get('node_id'))} freshness is {freshness}.",
                recommended_action="review_or_refresh_stale_lineage_artifact_upstream",
                family=_text(node.get("family")),
                node_id=_text(node.get("node_id")),
            )

    status = FAIL_STATUS if blocking_issues else WARN_STATUS if warning_issues else PASS_STATUS
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": VALIDATION_REPORT_TYPE,
        "as_of": _text(payload.get("as_of")),
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "purpose": (
            "Validate the artifact lineage graph required families, edges, and safety boundary."
        ),
        "status": status,
        "validation_status": status,
        "output_decision": status,
        "next_action": _next_action(status),
        "production_effect": PRODUCTION_EFFECT,
        "source_report_type": _text(payload.get("report_type")),
        "source_lineage_status": _text(payload.get("lineage_status"), _text(payload.get("status"))),
        "input_artifacts": _mapping(payload.get("input_artifacts")),
        "source_artifacts": _mapping(payload.get("input_artifacts")),
        "safety_boundary": _safety_boundary(),
        "limitations": [
            "Validation reads an existing artifact lineage graph only.",
            "Validation does not repair missing source artifacts or rerun upstream commands.",
            "Family-level PASS does not replace row-level data validation or PIT source review.",
        ],
        "blocking_issues": _dedupe_issues(blocking_issues),
        "warning_issues": _dedupe_issues(warning_issues),
        "checks": checks,
        "summary": {
            "check_count": len(checks),
            "failed_check_count": len(
                [check for check in checks if check.get("status") == FAIL_STATUS]
            ),
            "blocking_issue_count": len(_dedupe_issues(blocking_issues)),
            "warning_issue_count": len(_dedupe_issues(warning_issues)),
            "required_family_count": len(REQUIRED_FAMILIES),
            "required_edge_count": len(EDGE_SPECS),
        },
        "methodology": {
            "mode": "read_existing_lineage_graph_only",
            "does_not_run_upstream_commands": True,
            "does_not_refresh_data": True,
            "does_not_modify_production": True,
            "production_effect": PRODUCTION_EFFECT,
        },
    }


def write_artifact_lineage_json(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def write_artifact_lineage_markdown(payload: Mapping[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_artifact_lineage_markdown(payload), encoding="utf-8")
    return output_path


def write_artifact_lineage_validation_json(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    return write_artifact_lineage_json(payload, output_path)


def write_artifact_lineage_validation_markdown(
    payload: Mapping[str, Any],
    output_path: Path,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_artifact_lineage_validation_markdown(payload), encoding="utf-8")
    return output_path


def render_artifact_lineage_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Artifact Lineage Graph {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('lineage_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- node_count：{summary.get('node_count')}",
        f"- available_node_count：{summary.get('available_node_count')}",
        f"- required_family_count：{summary.get('required_family_count')}",
        f"- available_required_family_count：{summary.get('available_required_family_count')}",
        f"- required_edge_count：{summary.get('required_edge_count')}",
        f"- passing_required_edge_count：{summary.get('passing_required_edge_count')}",
        f"- blocking_issues：{summary.get('blocking_issue_count')}",
        f"- warning_issues：{summary.get('warning_issue_count')}",
        f"- next_action：{_text(payload.get('next_action'))}",
        "",
        "## Family Coverage",
        "",
        "|family|status|available_nodes|node_count|",
        "|---|---|---|---|",
    ]
    for family in _records(payload.get("family_coverage")):
        lines.append(
            f"|{_markdown_cell(family.get('family'))}|"
            f"{_markdown_cell(family.get('status'))}|"
            f"{_markdown_cell(family.get('available_node_count'))}|"
            f"{_markdown_cell(family.get('node_count'))}|"
        )
    lines.extend(
        [
            "",
            "## Required Edges",
            "",
            "|from_family|to_family|relationship|status|",
            "|---|---|---|---|",
        ]
    )
    for edge in _records(payload.get("edges")):
        lines.append(
            f"|{_markdown_cell(edge.get('from_family'))}|"
            f"{_markdown_cell(edge.get('to_family'))}|"
            f"{_markdown_cell(edge.get('relationship'))}|"
            f"{_markdown_cell(edge.get('status'))}|"
        )
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|scope|family|node_id|message|recommended_action|",
            "|---|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|artifact_lineage|||无阻断项。||")
    lines.extend(
        [
            "",
            "## Nodes",
            "",
            "|node_id|family|exists|status|freshness|path|",
            "|---|---|---|---|---|---|",
        ]
    )
    for node in _records(payload.get("nodes")):
        lines.append(
            f"|{_markdown_cell(node.get('node_id'))}|"
            f"{_markdown_cell(node.get('family'))}|"
            f"{_markdown_cell(node.get('exists'))}|"
            f"{_markdown_cell(node.get('status'))}|"
            f"{_markdown_cell(node.get('freshness_status'))}|"
            f"{_markdown_cell(node.get('artifact_path'))}|"
        )
    lines.extend(
        [
            "",
            "## Methodology",
            "",
            "本 graph 只读取 report registry、report index 和既有 artifact paths；"
            "不运行上游、不刷新数据、不补造 artifact、不修改 paper-shadow / owner review / "
            "production state。",
            "",
        ]
    )
    return "\n".join(lines)


def render_artifact_lineage_validation_markdown(payload: Mapping[str, Any]) -> str:
    summary = _mapping(payload.get("summary"))
    lines = [
        f"# Artifact Lineage Validation {payload.get('as_of')}",
        "",
        f"- 状态：{_text(payload.get('validation_status'), 'UNKNOWN')}",
        f"- production_effect：{_text(payload.get('production_effect'), PRODUCTION_EFFECT)}",
        f"- checks：{summary.get('check_count')}",
        f"- failed_checks：{summary.get('failed_check_count')}",
        f"- blocking_issues：{summary.get('blocking_issue_count')}",
        f"- warning_issues：{summary.get('warning_issue_count')}",
        "",
        "## Checks",
        "",
        "|check_id|status|severity|message|",
        "|---|---|---|---|",
    ]
    for check in _records(payload.get("checks")):
        lines.append(
            f"|{_markdown_cell(check.get('check_id'))}|"
            f"{_markdown_cell(check.get('status'))}|"
            f"{_markdown_cell(check.get('severity'))}|"
            f"{_markdown_cell(check.get('message'))}|"
        )
    lines.extend(
        [
            "",
            "## Blocking Issues",
            "",
            "|issue_id|scope|family|node_id|message|recommended_action|",
            "|---|---|---|---|---|---|",
        ]
    )
    for issue in _records(payload.get("blocking_issues")):
        lines.append(_issue_row(issue))
    if not _records(payload.get("blocking_issues")):
        lines.append("|NONE|artifact_lineage_validation|||无阻断项。||")
    lines.extend([""])
    return "\n".join(lines)


def _lineage_nodes(
    *,
    report_index_payload: Mapping[str, Any],
    project_root: Path,
) -> list[dict[str, Any]]:
    report_records = {
        _text(report.get("report_id")): report
        for report in _records(report_index_payload.get("reports"))
    }
    nodes: list[dict[str, Any]] = []
    for spec in FAMILY_SPECS:
        for report_id in spec.report_ids:
            report = report_records.get(report_id)
            if report is None:
                continue
            nodes.append(_node_from_report(spec=spec, report=report, project_root=project_root))
        for pattern in spec.path_patterns:
            path = _resolve_path(pattern, project_root)
            nodes.append(_node_from_path(spec=spec, path=path, source_pattern=pattern))
        if not any(node["family"] == spec.family_id for node in nodes):
            nodes.append(_missing_family_node(spec))
    return sorted(nodes, key=lambda node: (_text(node.get("family")), _text(node.get("node_id"))))


def _lineage_edges(nodes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    representative = {
        family: _representative_node(nodes, family)
        for family in REQUIRED_FAMILIES
    }
    edges: list[dict[str, Any]] = []
    for spec in EDGE_SPECS:
        source = representative.get(spec.from_family)
        target = representative.get(spec.to_family)
        source_id = "" if source is None else _text(source.get("node_id"))
        target_id = "" if target is None else _text(target.get("node_id"))
        source_available = bool(source is not None and source.get("exists"))
        target_available = bool(target is not None and target.get("exists"))
        status = PASS_STATUS if source_available and target_available else "MISSING_NODE"
        edges.append(
            {
                "edge_id": f"{spec.from_family}__to__{spec.to_family}",
                "from_family": spec.from_family,
                "to_family": spec.to_family,
                "from_node_id": source_id,
                "to_node_id": target_id,
                "relationship": spec.relationship,
                "required": True,
                "status": status,
                "production_effect": PRODUCTION_EFFECT,
            }
        )
    return edges


def _node_from_report(
    *,
    spec: FamilySpec,
    report: Mapping[str, Any],
    project_root: Path,
) -> dict[str, Any]:
    report_id = _text(report.get("report_id"), "UNKNOWN_REPORT")
    artifact_path = _resolve_path(_text(report.get("latest_artifact_path")), project_root)
    artifact_ref = ArtifactRef.from_path(artifact_path) if artifact_path is not None else None
    json_payload = _read_json_object(artifact_path)
    production_effect = (
        _text(json_payload.get("production_effect"))
        or _text(report.get("artifact_production_effect"))
        or PRODUCTION_EFFECT
    )
    return {
        "node_id": _safe_node_id(spec.family_id, report_id),
        "family": spec.family_id,
        "family_label": spec.label,
        "report_id": report_id,
        "title": _text(report.get("title"), report_id),
        "artifact_path": "" if artifact_path is None else str(artifact_path),
        "exists": (
            bool(report.get("exists")) and artifact_path is not None and artifact_path.exists()
        ),
        "status": _text(
            json_payload.get("status"),
            _text(report.get("artifact_status"), "UNKNOWN"),
        ),
        "freshness_status": _text(report.get("freshness_status"), "UNKNOWN"),
        "production_effect": production_effect,
        "sha256": (
            ""
            if artifact_ref is None or artifact_ref.sha256 is None
            else artifact_ref.sha256
        ),
        "size_bytes": None if artifact_ref is None else artifact_ref.size_bytes,
        "artifact_type": "" if artifact_ref is None else artifact_ref.artifact_type,
        "source": "report_index",
        "required_family": True,
        "reader_impact": _reader_impact(spec.family_id),
    }


def _node_from_path(*, spec: FamilySpec, path: Path, source_pattern: str) -> dict[str, Any]:
    artifact_ref = ArtifactRef.from_path(path)
    status = "AVAILABLE" if artifact_ref.exists else "MISSING"
    return {
        "node_id": _safe_node_id(spec.family_id, source_pattern),
        "family": spec.family_id,
        "family_label": spec.label,
        "report_id": "",
        "title": source_pattern,
        "artifact_path": str(path),
        "exists": artifact_ref.exists,
        "status": status,
        "freshness_status": "AVAILABLE" if artifact_ref.exists else "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "sha256": "" if artifact_ref.sha256 is None else artifact_ref.sha256,
        "size_bytes": artifact_ref.size_bytes,
        "artifact_type": artifact_ref.artifact_type,
        "source": "configured_data_path",
        "required_family": True,
        "reader_impact": _reader_impact(spec.family_id),
    }


def _missing_family_node(spec: FamilySpec) -> dict[str, Any]:
    return {
        "node_id": _safe_node_id(spec.family_id, "missing"),
        "family": spec.family_id,
        "family_label": spec.label,
        "report_id": "",
        "title": f"{spec.label} missing",
        "artifact_path": "",
        "exists": False,
        "status": "MISSING",
        "freshness_status": "MISSING",
        "production_effect": PRODUCTION_EFFECT,
        "sha256": "",
        "size_bytes": None,
        "artifact_type": "missing",
        "source": "required_family_placeholder",
        "required_family": True,
        "reader_impact": _reader_impact(spec.family_id),
    }


def _representative_node(
    nodes: Sequence[Mapping[str, Any]],
    family: str,
) -> Mapping[str, Any] | None:
    family_nodes = [node for node in nodes if _text(node.get("family")) == family]
    if not family_nodes:
        return None
    existing = [node for node in family_nodes if bool(node.get("exists"))]
    candidates = existing or family_nodes
    return sorted(candidates, key=lambda node: _text(node.get("node_id")))[0]


def _family_coverage(nodes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    coverage: list[dict[str, Any]] = []
    for spec in FAMILY_SPECS:
        family_nodes = [node for node in nodes if _text(node.get("family")) == spec.family_id]
        available = [node for node in family_nodes if bool(node.get("exists"))]
        coverage.append(
            {
                "family": spec.family_id,
                "label": spec.label,
                "status": "AVAILABLE" if available else "MISSING",
                "node_count": len(family_nodes),
                "available_node_count": len(available),
                "required": True,
            }
        )
    return coverage


def _summary(
    *,
    nodes: Sequence[Mapping[str, Any]],
    edges: Sequence[Mapping[str, Any]],
    validation: Mapping[str, Any],
) -> dict[str, Any]:
    family_coverage = _family_coverage(nodes)
    return {
        "node_count": len(nodes),
        "available_node_count": len([node for node in nodes if bool(node.get("exists"))]),
        "edge_count": len(edges),
        "required_family_count": len(REQUIRED_FAMILIES),
        "available_required_family_count": len(
            [family for family in family_coverage if family["status"] == "AVAILABLE"]
        ),
        "required_edge_count": len(EDGE_SPECS),
        "passing_required_edge_count": len(
            [edge for edge in edges if edge.get("required") and edge.get("status") == PASS_STATUS]
        ),
        "blocking_issue_count": len(_records(validation.get("blocking_issues"))),
        "warning_issue_count": len(_records(validation.get("warning_issues"))),
    }


def _append_check(
    checks: list[dict[str, Any]],
    issues: list[dict[str, Any]],
    *,
    check_id: str,
    passed: bool,
    severity: str,
    message: str,
    recommended_action: str,
    family: str = "",
    node_id: str = "",
    details: Mapping[str, Any] | None = None,
) -> None:
    status = PASS_STATUS if passed else WARN_STATUS if severity == "WARNING" else FAIL_STATUS
    checks.append(
        {
            "check_id": check_id,
            "status": status,
            "severity": severity,
            "message": message,
            "recommended_action": recommended_action,
            "family": family,
            "node_id": node_id,
            "details": {} if details is None else dict(details),
        }
    )
    if passed:
        return
    issues.append(
        {
            "issue_id": check_id,
            "severity": severity,
            "scope": "artifact_lineage",
            "family": family,
            "node_id": node_id,
            "message": message,
            "recommended_action": recommended_action,
        }
    )


def _dedupe_issues(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for record in records:
        key = (
            _text(record.get("issue_id")),
            _text(record.get("family")),
            _text(record.get("node_id")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(record))
    return deduped


def _read_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or path.suffix.lower() != ".json" or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _resolve_path(raw: str, project_root: Path) -> Path | None:
    if not raw:
        return None
    path = Path(raw)
    return path if path.is_absolute() else project_root / path


def _safety_boundary() -> dict[str, Any]:
    return {
        "mode": "read_existing_artifacts_only",
        "does_not_run_upstream_commands": True,
        "does_not_refresh_data": True,
        "does_not_generate_missing_artifacts": True,
        "does_not_modify_research_decisions": True,
        "does_not_modify_production": True,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": PRODUCTION_EFFECT,
    }


def _next_action(status: str) -> str:
    if status == FAIL_STATUS:
        return "restore_missing_lineage_artifacts_or_edges_before_paper_shadow_decision_audit"
    if status == WARN_STATUS:
        return "review_stale_lineage_artifacts_before_using_chain_as_complete_audit_context"
    return "continue_candidate_research_chain_audit"


def _reader_impact(family: str) -> str:
    impacts = {
        "data_artifacts": (
            "Shows whether downstream candidate evidence had validated market/macro inputs."
        ),
        "cache_catalog": (
            "Shows cache checksum and completeness context before feature interpretation."
        ),
        "refresh_audit": "Shows whether data refresh and validation evidence exists for the chain.",
        "pit_manifest": (
            "Shows source-level PIT visibility limitations before historical interpretation."
        ),
        "signal_artifacts": (
            "Shows signal input availability before paper-shadow daily observations."
        ),
        "daily_paper_shadow": "Shows daily observation evidence feeding drift and weekly review.",
        "drift_monitor": "Shows whether observed behavior drifted from expectations.",
        "weekly_review": "Shows weekly paper-shadow decision context.",
        "staleness_monitor": "Shows whether evidence age blocks continued shadow observation.",
        "readiness_reports": "Shows whether the chain is safe to continue or needs owner review.",
        "owner_reviews": "Shows manual owner review or decision checkpoints.",
    }
    return impacts.get(family, "Lineage audit context.")


def _records(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _safe_node_id(family: str, value: str) -> str:
    token = "".join(char.lower() if char.isalnum() else "_" for char in value)
    token = "_".join(part for part in token.split("_") if part) or "node"
    return f"{family}:{token}"


def _markdown_cell(value: Any) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _issue_row(issue: Mapping[str, Any]) -> str:
    return (
        f"|{_markdown_cell(issue.get('issue_id'))}|"
        f"{_markdown_cell(issue.get('scope'))}|"
        f"{_markdown_cell(issue.get('family'))}|"
        f"{_markdown_cell(issue.get('node_id'))}|"
        f"{_markdown_cell(issue.get('message'))}|"
        f"{_markdown_cell(issue.get('recommended_action'))}|"
    )
