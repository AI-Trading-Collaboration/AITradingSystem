from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from html import escape
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "1.0"
REPORT_TYPE = "parameter_governance_web_view"
TASK_ID = "TRADING-020"
SUMMARY_TASK_ID = "TRADING-019"
MODE = "parameter_governance_web_view_only"
PRODUCTION_EFFECT_NONE = "none"

RENDERED = "RENDERED"
SAFETY_BLOCKED = "SAFETY_BLOCKED"
INPUT_MISSING = "INPUT_MISSING"
INPUT_INVALID = "INPUT_INVALID"
ERROR = "ERROR"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA_ROOT = REPO_ROOT / "data"

PIPELINE_CONTRACT: dict[str, Any] = {
    "reads_governance_summary_only": True,
    "runs_shadow_iteration_pipeline": False,
    "runs_comparison_pipeline": False,
    "runs_multi_day_review_pipeline": False,
    "runs_promotion_proposal_pipeline": False,
    "runs_apply_preflight_pipeline": False,
    "runs_promotion_apply": False,
    "runs_promotion_rollback": False,
    "runs_lifecycle_audit_pipeline": False,
    "runs_governance_summary_pipeline": False,
    "runs_web_view_render_script": False,
    "runs_scoring_pipeline": False,
    "runs_broker_runner": False,
    "runs_paper_runner": False,
    "runs_replay_runner": False,
    "writes_production_profile": False,
    "writes_production_weights": False,
    "writes_shadow_weights": False,
    "writes_approved_profile": False,
    "promotes_shadow_to_production": False,
    "changes_daily_dashboard_main_conclusion": False,
    "triggers_trade": False,
    "production_effect": PRODUCTION_EFFECT_NONE,
    "manual_review_only": True,
    "governance_only": True,
    "web_view_only": True,
}


def default_governance_root(data_root: Path) -> Path:
    return data_root / "derived" / "weight_iterations" / "governance"


def default_governance_summary_json_path(data_root: Path, as_of: date) -> Path:
    return default_governance_root(data_root) / (
        f"parameter_governance_summary_{as_of.isoformat()}.json"
    )


def default_web_view_root(data_root: Path) -> Path:
    return default_governance_root(data_root) / "web"


def default_web_view_html_path(data_root: Path, as_of: date) -> Path:
    return default_web_view_root(data_root) / (
        f"parameter_governance_web_view_{as_of.isoformat()}.html"
    )


def default_web_view_metadata_path(data_root: Path, as_of: date) -> Path:
    return default_web_view_root(data_root) / (
        f"parameter_governance_web_view_{as_of.isoformat()}.json"
    )


def latest_governance_summary_json_path(data_root: Path, as_of: date | None = None) -> Path | None:
    governance_root = default_governance_root(data_root)
    if not governance_root.exists():
        return None
    candidates: list[tuple[date, Path]] = []
    for path in governance_root.glob("parameter_governance_summary_*.json"):
        raw_date = path.stem.removeprefix("parameter_governance_summary_")
        parsed = _parse_iso_date(raw_date)
        if parsed is not None and (as_of is None or parsed <= as_of):
            candidates.append((parsed, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[0])[1]


def write_parameter_governance_web_view(
    *,
    as_of: date | None = None,
    data_root: Path = DEFAULT_DATA_ROOT,
    governance_summary_file: Path | None = None,
    output_file: Path | None = None,
    metadata_file: Path | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(tz=UTC)
    generated_date = generated.date()
    summary_path = _resolve_summary_path(
        data_root=data_root,
        as_of=as_of,
        governance_summary_file=governance_summary_file,
        generated_date=generated_date,
    )

    try:
        payload, input_decision, input_reason = _load_summary(summary_path)
        output_date = _resolve_output_date(
            as_of=as_of,
            payload=payload,
            input_path=summary_path,
            generated_date=generated_date,
        )
        html_path = output_file or default_web_view_html_path(data_root, output_date)
        metadata_path = metadata_file or default_web_view_metadata_path(data_root, output_date)

        safety = _summary_safety_validation(payload, input_decision=input_decision)
        if input_decision != RENDERED:
            render_decision = input_decision
            html = _render_non_rendered_html(
                render_decision=render_decision,
                reason=input_reason,
                metadata=None,
                payload=payload,
            )
        elif safety.get("summary_task_id_valid") is not True:
            render_decision = INPUT_INVALID
            html = _render_non_rendered_html(
                render_decision=render_decision,
                reason="Governance summary task_id must be TRADING-019.",
                metadata=None,
                payload=payload,
            )
        elif safety.get("status") != "PASS":
            render_decision = SAFETY_BLOCKED
            html = _render_non_rendered_html(
                render_decision=render_decision,
                reason=(
                    "Web view render blocked because governance summary safety fields are "
                    "invalid."
                ),
                metadata=None,
                payload=payload,
            )
        else:
            render_decision = RENDERED
            html = render_parameter_governance_web_view_html(payload)

        metadata = build_parameter_governance_web_view_metadata(
            as_of=output_date,
            input_path=summary_path,
            html_path=html_path,
            metadata_path=metadata_path,
            render_decision=render_decision,
            payload=payload,
            safety_validation=safety,
            generated_at=generated,
        )
        if render_decision != RENDERED:
            html = _render_non_rendered_html(
                render_decision=render_decision,
                reason=input_reason or "; ".join(_strings(safety.get("blocking_reasons"))),
                metadata=metadata,
                payload=payload,
            )
        _write_text(html_path, html)
        _write_json(metadata_path, metadata)
        _assert_web_view_safety_invariants(metadata)
        return metadata
    except Exception as exc:  # pragma: no cover - defensive report path
        output_date = as_of or generated_date
        html_path = output_file or default_web_view_html_path(data_root, output_date)
        metadata_path = metadata_file or default_web_view_metadata_path(data_root, output_date)
        safety = _empty_safety_validation(status="FAIL", reason=str(exc))
        metadata = build_parameter_governance_web_view_metadata(
            as_of=output_date,
            input_path=summary_path,
            html_path=html_path,
            metadata_path=metadata_path,
            render_decision=ERROR,
            payload={},
            safety_validation=safety,
            generated_at=generated,
        )
        html = _render_non_rendered_html(
            render_decision=ERROR,
            reason=str(exc),
            metadata=metadata,
            payload={},
        )
        _write_text(html_path, html)
        _write_json(metadata_path, metadata)
        _assert_web_view_safety_invariants(metadata)
        return metadata


def build_parameter_governance_web_view_metadata(
    *,
    as_of: date,
    input_path: Path,
    html_path: Path,
    metadata_path: Path,
    render_decision: str,
    payload: dict[str, Any],
    safety_validation: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    render_summary = _render_summary(payload)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "report_type": REPORT_TYPE,
        "task_id": TASK_ID,
        "date": as_of.isoformat(),
        "generated_at": _isoformat_z(generated_at),
        "mode": MODE,
        "render_decision": render_decision,
        "production_effect": PRODUCTION_EFFECT_NONE,
        "manual_review_only": True,
        "governance_only": True,
        "web_view_only": True,
        "apply_executed_by_web_view": False,
        "rollback_executed_by_web_view": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "input_artifacts": {
            "governance_summary": _artifact_record(input_path),
        },
        "output_artifacts": {
            "html": {"path": str(html_path)},
            "metadata": {"path": str(metadata_path)},
        },
        "render_summary": render_summary,
        "safety_validation": safety_validation,
        "pipeline_contract": dict(PIPELINE_CONTRACT),
        "audit": {
            "created_by": "scripts/render_parameter_governance_web_view.py",
            "created_at": _isoformat_z(generated_at),
            "read_only": True,
            "no_files_modified_except_web_view_artifacts": True,
        },
    }
    _assert_web_view_safety_invariants(metadata)
    return metadata


def render_parameter_governance_web_view_html(payload: dict[str, Any]) -> str:
    governance_state = _string_value(payload.get("governance_state")) or "MISSING"
    action_level = _string_value(payload.get("action_level")) or "NONE"
    class_name = _state_class(governance_state, action_level)
    title = "Parameter Governance Dashboard"
    critical = _audit_list(payload, "critical_findings")

    sections = [
        "<!doctype html>",
        '<html lang="en">',
        "<head>",
        '<meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>{_text(title)}</title>",
        f"<style>{_CSS}</style>",
        "</head>",
        f'<body class="{class_name}">',
        "<header>",
        f"<h1>{_text(title)}</h1>",
        '<div class="overview-grid">',
        _summary_tile("Date", payload.get("date", "")),
        _summary_tile("Governance State", governance_state),
        _summary_tile("Action Required", str(payload.get("action_required") is True).lower()),
        _summary_tile("Action Level", action_level),
        _summary_tile("Recommended Action", payload.get("recommended_action", "")),
        "</div>",
        "</header>",
        "<main>",
        _render_safety_banner(payload, critical),
        _render_read_only_metadata(),
        _render_weights_section(payload),
        _render_shadow_review_section(payload),
        _render_promotion_timeline_section(payload),
        _render_pending_items_section(payload),
        _render_safety_audit_section(payload),
        _render_findings_section(payload),
        _render_artifacts_section(payload),
        "</main>",
        "<footer>",
        "Static read-only web view. No apply, rollback, broker, replay, or trading execution.",
        "</footer>",
        "</body>",
        "</html>",
        "",
    ]
    return "\n".join(sections)


def _render_non_rendered_html(
    *,
    render_decision: str,
    reason: str,
    metadata: dict[str, Any] | None,
    payload: dict[str, Any],
) -> str:
    render_summary = _mapping(metadata.get("render_summary") if metadata else {})
    safety = _mapping(metadata.get("safety_validation") if metadata else {})
    blocking = _strings(safety.get("blocking_reasons"))
    title = "Parameter Governance Web View"
    message = {
        SAFETY_BLOCKED: (
            "Web view render blocked because governance summary safety fields are invalid."
        ),
        INPUT_MISSING: "Governance summary input is missing.",
        INPUT_INVALID: "Governance summary input is invalid.",
        ERROR: "Web view render failed.",
    }.get(render_decision, "Web view was not rendered.")
    return "\n".join(
        [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{_text(title)}</title>",
            f"<style>{_CSS}</style>",
            "</head>",
            '<body class="urgent">',
            "<header>",
            f"<h1>{_text(title)}</h1>",
            '<div class="banner urgent-banner">',
            f"<strong>{_text(render_decision)}</strong>",
            f"<p>{_text(message)}</p>",
            f"<p>{_text(reason)}</p>",
            "</div>",
            "</header>",
            "<main>",
            _render_read_only_metadata(),
            "<section>",
            "<h2>Render Summary</h2>",
            '<div class="overview-grid">',
            _summary_tile("render_decision", render_decision),
            _summary_tile("governance_state", render_summary.get("governance_state", "MISSING")),
            _summary_tile("action_level", render_summary.get("action_level", "NONE")),
            _summary_tile(
                "safety_boundary_status",
                render_summary.get("safety_boundary_status", "MISSING"),
            ),
            "</div>",
            "</section>",
            _render_blocking_reasons(blocking),
            _render_findings_section(payload),
            _render_artifacts_section(payload),
            "</main>",
            "<footer>",
            (
                "Static read-only blocked report. No apply, rollback, broker, replay, "
                "or trading execution."
            ),
            "</footer>",
            "</body>",
            "</html>",
            "",
        ]
    )


def _render_safety_banner(payload: dict[str, Any], critical: list[str]) -> str:
    governance_state = _string_value(payload.get("governance_state"))
    safety = _mapping(payload.get("safety_boundary_audit"))
    if governance_state == "SAFETY_ANOMALY":
        return "\n".join(
            [
                '<section class="banner urgent-banner">',
                "<h2>URGENT: Safety Anomaly Detected</h2>",
                _unordered_list(critical or ["Safety anomaly detected."]),
                "</section>",
            ]
        )
    return "\n".join(
        [
            '<section class="banner safe-banner">',
            "<h2>Safety Boundary: PASS</h2>",
            "<p>No broker/replay/trading execution detected.</p>",
            (
                "<p>Summary safety boundary status: "
                f"<strong>{_text(safety.get('status', 'MISSING'))}</strong></p>"
            ),
            "</section>",
        ]
    )


def _render_read_only_metadata() -> str:
    return "\n".join(
        [
            '<section aria-labelledby="readonly-metadata-title">',
            '<h2 id="readonly-metadata-title">Read-only Metadata</h2>',
            '<div class="metadata-grid">',
            _summary_tile("production_effect", PRODUCTION_EFFECT_NONE),
            _summary_tile("manual_review_only", "true"),
            _summary_tile("web_view_only", "true"),
            _summary_tile("governance_only", "true"),
            _summary_tile("apply_executed_by_web_view", "false"),
            _summary_tile("rollback_executed_by_web_view", "false"),
            _summary_tile("broker_execution", "false"),
            _summary_tile("replay_execution", "false"),
            _summary_tile("trading_execution", "false"),
            "</div>",
            "</section>",
        ]
    )


def _render_weights_section(payload: dict[str, Any]) -> str:
    production = _mapping(payload.get("production_state"))
    shadow = _mapping(payload.get("shadow_state"))
    production_weights = _mapping(production.get("weights"))
    shadow_weights = _mapping(shadow.get("weights"))
    delta = _mapping(shadow.get("delta_from_production"))
    keys = sorted(set(production_weights) | set(shadow_weights) | set(delta))
    if not keys:
        keys = ["NOT_AVAILABLE"]
    mismatch = (
        production_weights
        and shadow_weights
        and set(production_weights.keys()) != set(shadow_weights.keys())
    )
    rows = [
        "<tr><th>Weight Key</th><th>Production</th><th>Shadow</th><th>Delta</th></tr>",
    ]
    for key in keys:
        rows.append(
            "<tr>"
            f"<td>{_text(key)}</td>"
            f'<td class="num">{_text(_format_weight(production_weights.get(key)))}</td>'
            f'<td class="num">{_text(_format_weight(shadow_weights.get(key)))}</td>'
            f'<td class="num">{_text(_format_signed_weight(delta.get(key)))}</td>'
            "</tr>"
        )
    warnings: list[str] = []
    if not production_weights:
        warnings.append("Production weights are NOT_AVAILABLE.")
    if not shadow_weights:
        warnings.append("Shadow weights are NOT_AVAILABLE.")
    if mismatch:
        warnings.append("Weight key mismatch warning: production and shadow keys differ.")
    return "\n".join(
        [
            '<section aria-labelledby="weights-title">',
            '<h2 id="weights-title">Production vs Shadow Weights</h2>',
            _warning_panel(warnings),
            '<div class="table-wrap">',
            "<table>",
            *rows,
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _render_shadow_review_section(payload: dict[str, Any]) -> str:
    review = _mapping(payload.get("shadow_vs_production_review"))
    if review.get("status") in {"", None, "MISSING"}:
        body = "<p>Latest multi-day review not found.</p>"
    else:
        body = "\n".join(
            [
                '<div class="metadata-grid">',
                _summary_tile("Review Decision", review.get("review_decision", "MISSING")),
                _summary_tile(
                    "Available Comparison Days",
                    review.get("available_comparison_days", 0),
                ),
                _summary_tile("Average Score Delta", review.get("average_score_delta", 0.0)),
                _summary_tile(
                    "Decision Difference Count",
                    review.get("decision_difference_count", 0),
                ),
                _summary_tile("Risk Flag Delta Total", review.get("risk_flag_delta_total", 0)),
                "</div>",
            ]
        )
    return "\n".join(
        [
            '<section aria-labelledby="shadow-review-title">',
            '<h2 id="shadow-review-title">Shadow Review Status</h2>',
            body,
            "</section>",
        ]
    )


def _render_promotion_timeline_section(payload: dict[str, Any]) -> str:
    promotion = _mapping(payload.get("promotion_status"))
    artifacts = _mapping(payload.get("input_artifacts"))
    stages = (
        (
            "Proposal",
            promotion.get("proposal_status", "MISSING"),
            promotion.get("proposal_decision", "MISSING"),
            promotion.get("promotion_proposed") is True,
            _artifact_path(artifacts, "latest_promotion_proposal"),
        ),
        (
            "Preflight",
            promotion.get("preflight_status", "MISSING"),
            promotion.get("preflight_decision", "MISSING"),
            False,
            _artifact_path(artifacts, "latest_apply_preflight"),
        ),
        (
            "Apply",
            promotion.get("apply_status", "MISSING"),
            promotion.get("apply_decision", "MISSING"),
            promotion.get("apply_executed") is True,
            _artifact_path(artifacts, "latest_apply_result"),
        ),
        (
            "Rollback",
            promotion.get("rollback_status", "MISSING"),
            promotion.get("rollback_decision", "MISSING"),
            promotion.get("rollback_executed") is True,
            _artifact_path(artifacts, "latest_rollback_result"),
        ),
        (
            "Lifecycle Audit",
            promotion.get("lifecycle_status", "MISSING"),
            promotion.get("lifecycle_decision", "MISSING"),
            False,
            _artifact_path(artifacts, "latest_lifecycle_audit"),
        ),
    )
    cards = []
    for stage, status, decision, executed, path in stages:
        cards.append(
            '<article class="stage-card">'
            f"<h3>{_text(stage)}</h3>"
            f"<p><strong>Status:</strong> {_text(status)}</p>"
            f"<p><strong>Decision:</strong> {_text(decision)}</p>"
            f"<p><strong>Executed:</strong> {_text(str(executed).lower())}</p>"
            f"<p><strong>Artifact path:</strong> <code>{_text(path or 'MISSING')}</code></p>"
            "</article>"
        )
    return "\n".join(
        [
            '<section aria-labelledby="timeline-title">',
            '<h2 id="timeline-title">Promotion Lifecycle Timeline</h2>',
            '<div class="timeline-grid">',
            *cards,
            "</div>",
            "</section>",
        ]
    )


def _render_pending_items_section(payload: dict[str, Any]) -> str:
    pending = _mapping(payload.get("pending_items"))
    keys = (
        "pending_proposal_review",
        "pending_preflight",
        "pending_apply",
        "pending_rollback",
        "pending_lifecycle_audit",
    )
    rows = ["<tr><th>Pending Item</th><th>Value</th></tr>"]
    for key in keys:
        value = pending.get(key) is True
        class_name = "pending-true" if value else ""
        rows.append(
            f'<tr class="{class_name}"><td>{_text(key)}</td>'
            f"<td>{_text(str(value).lower())}</td></tr>"
        )
    notice = (
        '<p class="attention">Manual approval/apply may be required.</p>'
        if pending.get("pending_apply") is True
        else ""
    )
    return "\n".join(
        [
            '<section aria-labelledby="pending-title">',
            '<h2 id="pending-title">Pending Items</h2>',
            notice,
            '<div class="table-wrap">',
            "<table>",
            *rows,
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _render_safety_audit_section(payload: dict[str, Any]) -> str:
    safety = _mapping(payload.get("safety_boundary_audit"))
    blocking = _strings(safety.get("blocking_reasons"))
    rows = [
        ("safety_boundary_audit.status", safety.get("status", "MISSING")),
        (
            "latest_lifecycle_has_safety_anomaly",
            str(safety.get("latest_lifecycle_has_safety_anomaly") is True).lower(),
        ),
        ("broker_execution", str(payload.get("broker_execution") is True).lower()),
        ("replay_execution", str(payload.get("replay_execution") is True).lower()),
        ("trading_execution", str(payload.get("trading_execution") is True).lower()),
    ]
    table_rows = ["<tr><th>Check</th><th>Value</th></tr>"]
    for key, value in rows:
        table_rows.append(f"<tr><td>{_text(key)}</td><td>{_text(value)}</td></tr>")
    return "\n".join(
        [
            '<section aria-labelledby="safety-audit-title">',
            '<h2 id="safety-audit-title">Safety Boundary Audit</h2>',
            '<div class="table-wrap">',
            "<table>",
            *table_rows,
            "</table>",
            "</div>",
            _render_blocking_reasons(blocking),
            "</section>",
        ]
    )


def _render_findings_section(payload: dict[str, Any]) -> str:
    critical = _audit_list(payload, "critical_findings")
    warnings = _audit_list(payload, "warnings")
    notes = _audit_list(payload, "notes")
    return "\n".join(
        [
            '<section aria-labelledby="findings-title">',
            '<h2 id="findings-title">Findings / Warnings / Notes</h2>',
            '<div class="finding-grid">',
            '<article class="finding-card critical">',
            "<h3>Critical Findings</h3>",
            _unordered_list(critical or ["None."]),
            "</article>",
            '<article class="finding-card warning">',
            "<h3>Warnings</h3>",
            _unordered_list(warnings or ["None."]),
            "</article>",
            '<article class="finding-card note">',
            "<h3>Notes</h3>",
            _unordered_list(notes or ["None."]),
            "</article>",
            "</div>",
            "</section>",
        ]
    )


def _render_artifacts_section(payload: dict[str, Any]) -> str:
    artifacts = _mapping(payload.get("input_artifacts"))
    outputs = _mapping(payload.get("outputs"))
    rows = [
        ("governance summary markdown path", outputs.get("markdown", "")),
        ("018C2 review path", _artifact_path(artifacts, "latest_multi_day_review")),
        ("018D proposal path", _artifact_path(artifacts, "latest_promotion_proposal")),
        ("018E1 preflight path", _artifact_path(artifacts, "latest_apply_preflight")),
        ("018E2 apply result path", _artifact_path(artifacts, "latest_apply_result")),
        ("018E3 rollback result path", _artifact_path(artifacts, "latest_rollback_result")),
        ("018F lifecycle audit path", _artifact_path(artifacts, "latest_lifecycle_audit")),
    ]
    table_rows = ["<tr><th>Artifact</th><th>Path</th></tr>"]
    for label, path in rows:
        table_rows.append(
            f"<tr><td>{_text(label)}</td><td><code>{_text(path or 'MISSING')}</code></td></tr>"
        )
    return "\n".join(
        [
            '<section aria-labelledby="artifacts-title">',
            '<h2 id="artifacts-title">Artifact Links / Paths</h2>',
            '<div class="table-wrap">',
            "<table>",
            *table_rows,
            "</table>",
            "</div>",
            "</section>",
        ]
    )


def _render_blocking_reasons(blocking: list[str]) -> str:
    if not blocking:
        return ""
    return "\n".join(
        [
            '<section class="banner urgent-banner">',
            "<h2>Blocking Reasons</h2>",
            _unordered_list(blocking),
            "</section>",
        ]
    )


def _warning_panel(warnings: list[str]) -> str:
    if not warnings:
        return ""
    return "\n".join(
        [
            '<div class="warning-panel">',
            "<strong>Warnings</strong>",
            _unordered_list(warnings),
            "</div>",
        ]
    )


def _summary_tile(label: str, value: Any) -> str:
    return (
        '<div class="summary-tile">'
        f"<span>{_text(label)}</span>"
        f"<strong>{_text(value)}</strong>"
        "</div>"
    )


def _unordered_list(items: list[str]) -> str:
    return "<ul>" + "".join(f"<li>{_text(item)}</li>" for item in items) + "</ul>"


def _resolve_summary_path(
    *,
    data_root: Path,
    as_of: date | None,
    governance_summary_file: Path | None,
    generated_date: date,
) -> Path:
    if governance_summary_file is not None:
        return governance_summary_file
    if as_of is not None:
        return default_governance_summary_json_path(data_root, as_of)
    latest = latest_governance_summary_json_path(data_root)
    return latest or default_governance_summary_json_path(data_root, generated_date)


def _load_summary(path: Path) -> tuple[dict[str, Any], str, str]:
    if not path.exists() or not path.is_file():
        return {}, INPUT_MISSING, f"Governance summary not found: {path}"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {}, INPUT_INVALID, f"Governance summary JSON is invalid: {exc}"
    except OSError as exc:
        return {}, INPUT_INVALID, f"Governance summary cannot be read: {exc}"
    if not isinstance(payload, dict):
        return {}, INPUT_INVALID, "Governance summary JSON must be an object."
    return payload, RENDERED, ""


def _summary_safety_validation(
    payload: dict[str, Any],
    *,
    input_decision: str,
) -> dict[str, Any]:
    if input_decision == INPUT_MISSING:
        return _empty_safety_validation(status="FAIL", reason="governance_summary_missing")
    if input_decision == INPUT_INVALID:
        return _empty_safety_validation(status="FAIL", reason="governance_summary_invalid")

    summary_task_id_valid = payload.get("task_id") == SUMMARY_TASK_ID
    summary_production_effect_none = payload.get("production_effect") == PRODUCTION_EFFECT_NONE
    summary_manual_review_only = payload.get("manual_review_only") is True
    summary_governance_only = payload.get("governance_only") is True
    summary_apply_not_executed = payload.get("apply_executed_by_governance") is False
    summary_rollback_not_executed = payload.get("rollback_executed_by_governance") is False
    summary_broker_false = payload.get("broker_execution") is False
    summary_replay_false = payload.get("replay_execution") is False
    summary_trading_false = payload.get("trading_execution") is False
    summary_no_execution_flags = all(
        (
            summary_apply_not_executed,
            summary_rollback_not_executed,
            summary_broker_false,
            summary_replay_false,
            summary_trading_false,
        )
    )
    checks = {
        "summary_task_id_valid": summary_task_id_valid,
        "summary_production_effect_none": summary_production_effect_none,
        "summary_manual_review_only": summary_manual_review_only,
        "summary_governance_only": summary_governance_only,
        "summary_apply_not_executed": summary_apply_not_executed,
        "summary_rollback_not_executed": summary_rollback_not_executed,
        "summary_broker_execution_false": summary_broker_false,
        "summary_replay_execution_false": summary_replay_false,
        "summary_trading_execution_false": summary_trading_false,
        "summary_no_execution_flags": summary_no_execution_flags,
    }
    blocking = [key for key, passed in checks.items() if passed is not True]
    return {
        "status": "PASS" if not blocking else "FAIL",
        **checks,
        "blocking_reasons": blocking,
    }


def _empty_safety_validation(*, status: str, reason: str) -> dict[str, Any]:
    return {
        "status": status,
        "summary_task_id_valid": False,
        "summary_production_effect_none": False,
        "summary_manual_review_only": False,
        "summary_governance_only": False,
        "summary_apply_not_executed": False,
        "summary_rollback_not_executed": False,
        "summary_broker_execution_false": False,
        "summary_replay_execution_false": False,
        "summary_trading_execution_false": False,
        "summary_no_execution_flags": False,
        "blocking_reasons": [reason],
    }


def _render_summary(payload: dict[str, Any]) -> dict[str, Any]:
    safety = _mapping(payload.get("safety_boundary_audit"))
    findings = _mapping(payload.get("audit_findings"))
    return {
        "governance_state": _string_value(payload.get("governance_state")) or "MISSING",
        "action_required": payload.get("action_required") is True,
        "action_level": _string_value(payload.get("action_level")) or "NONE",
        "safety_boundary_status": _string_value(safety.get("status")) or "MISSING",
        "critical_findings_count": len(_strings(findings.get("critical_findings"))),
        "warnings_count": len(_strings(findings.get("warnings"))),
    }


def _resolve_output_date(
    *,
    as_of: date | None,
    payload: dict[str, Any],
    input_path: Path,
    generated_date: date,
) -> date:
    if as_of is not None:
        return as_of
    payload_date = _parse_iso_date(_string_value(payload.get("date")))
    if payload_date is not None:
        return payload_date
    raw_date = input_path.stem.removeprefix("parameter_governance_summary_")
    path_date = _parse_iso_date(raw_date)
    return path_date or generated_date


def _artifact_record(path: Path) -> dict[str, Any]:
    exists = path.exists() and path.is_file()
    return {
        "status": "FOUND" if exists else "MISSING",
        "path": str(path),
        "exists": exists,
        "sha256": _sha256(path) if exists else "",
        "size_bytes": path.stat().st_size if exists else 0,
    }


def _artifact_path(artifacts: dict[str, Any], key: str) -> str:
    artifact = _mapping(artifacts.get(key))
    return _string_value(artifact.get("path"))


def _audit_list(payload: dict[str, Any], key: str) -> list[str]:
    findings = _mapping(payload.get("audit_findings"))
    return _strings(findings.get(key))


def _state_class(governance_state: str, action_level: str) -> str:
    if governance_state == "SAFETY_ANOMALY" or action_level == "URGENT":
        return "urgent"
    if governance_state in {"APPROVAL_REQUIRED", "REVIEW_REQUIRED"} or action_level in {
        "APPROVAL_REQUIRED",
        "REVIEW_REQUIRED",
        "ROLLBACK_REVIEW_REQUIRED",
    }:
        return "attention"
    if governance_state in {"SHADOW_LEARNING", "APPLIED_NEEDS_MONITORING"}:
        return "watch"
    return "normal"


def _format_weight(value: Any) -> str:
    parsed = _optional_float(value)
    return "NOT_AVAILABLE" if parsed is None else f"{parsed:.4f}"


def _format_signed_weight(value: Any) -> str:
    parsed = _optional_float(value)
    if parsed is None:
        return "NOT_AVAILABLE"
    sign = "+" if parsed >= 0 else ""
    return f"{sign}{parsed:.4f}"


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list):
        return [str(item) for item in value if str(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item)]
    if isinstance(value, set):
        return [str(item) for item in value if str(item)]
    return []


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _optional_float(value: Any) -> float | None:
    try:
        if isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _text(value: Any) -> str:
    return escape(str(value), quote=True)


def _isoformat_z(value: datetime) -> str:
    normalized = value.astimezone(UTC) if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return normalized.isoformat().replace("+00:00", "Z")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _assert_web_view_safety_invariants(payload: dict[str, Any]) -> None:
    if payload.get("production_effect") != PRODUCTION_EFFECT_NONE:
        raise ValueError("parameter governance web view production_effect must remain none")
    if payload.get("manual_review_only") is not True:
        raise ValueError("parameter governance web view must remain manual_review_only")
    if payload.get("governance_only") is not True:
        raise ValueError("parameter governance web view must remain governance_only")
    if payload.get("web_view_only") is not True:
        raise ValueError("parameter governance web view must remain web_view_only")
    if payload.get("apply_executed_by_web_view") is not False:
        raise ValueError("parameter governance web view must not execute apply")
    if payload.get("rollback_executed_by_web_view") is not False:
        raise ValueError("parameter governance web view must not execute rollback")
    for field in ("broker_execution", "replay_execution", "trading_execution"):
        if payload.get(field) is not False:
            raise ValueError(f"parameter governance web view must keep {field}=false")


_CSS = """
:root {
  color-scheme: light;
  --bg: #f6f8fb;
  --surface: #ffffff;
  --ink: #111827;
  --muted: #596579;
  --line: #d8e0ea;
  --ok: #0f766e;
  --warn: #a16207;
  --danger: #b91c1c;
  --attention: #7c3aed;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  background: var(--bg);
  color: var(--ink);
  font-family: Arial, "Microsoft YaHei", sans-serif;
  line-height: 1.5;
}
header, main, footer {
  width: min(1180px, calc(100% - 32px));
  margin: 0 auto;
}
header {
  padding: 28px 0 18px;
  border-bottom: 1px solid var(--line);
}
main {
  display: flex;
  flex-direction: column;
  gap: 18px;
  padding: 22px 0 34px;
}
h1, h2, h3, p { margin-top: 0; }
h1 { margin-bottom: 14px; font-size: 28px; letter-spacing: 0; }
h2 { margin-bottom: 12px; font-size: 20px; letter-spacing: 0; }
h3 { margin-bottom: 8px; font-size: 15px; letter-spacing: 0; }
section {
  background: var(--surface);
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 18px;
}
.overview-grid, .metadata-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
  gap: 10px;
}
.summary-tile {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcfe;
  padding: 10px 12px;
}
.summary-tile span {
  display: block;
  color: var(--muted);
  font-size: 12px;
}
.summary-tile strong {
  display: block;
  overflow-wrap: anywhere;
  font-size: 15px;
}
.banner {
  border-width: 2px;
}
.safe-banner {
  border-color: #99d4ca;
  background: #eef8f6;
}
.urgent-banner {
  border-color: #efa1a1;
  background: #fff1f1;
}
.warning-panel, .attention {
  border: 1px solid #f0c36d;
  border-radius: 6px;
  background: #fff8e8;
  color: #754c00;
  padding: 10px 12px;
}
.attention {
  font-weight: 700;
}
.table-wrap { overflow-x: auto; }
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}
th, td {
  border-bottom: 1px solid var(--line);
  padding: 9px 10px;
  text-align: left;
  vertical-align: top;
}
th { color: var(--muted); font-size: 12px; }
.num { text-align: right; font-variant-numeric: tabular-nums; }
code {
  font-family: Consolas, "Liberation Mono", monospace;
  font-size: 12px;
  overflow-wrap: anywhere;
}
.timeline-grid, .finding-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}
.stage-card, .finding-card {
  border: 1px solid var(--line);
  border-radius: 6px;
  background: #fbfcfe;
  padding: 12px;
}
.finding-card.critical {
  border-color: #efa1a1;
  background: #fff7f7;
}
.finding-card.warning {
  border-color: #f0c36d;
  background: #fffaf0;
}
.finding-card.note {
  border-color: #b7c8da;
}
.pending-true td {
  color: var(--danger);
  font-weight: 700;
}
ul { margin: 0 0 0 18px; padding: 0; }
li + li { margin-top: 4px; }
footer {
  border-top: 1px solid var(--line);
  color: var(--muted);
  padding: 16px 0 28px;
  font-size: 13px;
}
body.urgent header { border-bottom-color: var(--danger); }
body.attention header { border-bottom-color: var(--attention); }
body.watch header { border-bottom-color: var(--warn); }
body.normal header { border-bottom-color: var(--ok); }
@media (max-width: 760px) {
  header, main, footer { width: min(100% - 20px, 1180px); }
  h1 { font-size: 24px; }
}
"""
