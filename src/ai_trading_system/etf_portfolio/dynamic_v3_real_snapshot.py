from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.etf_portfolio.dynamic_v3_parameter_research import (
    DEFAULT_DYNAMIC_V3_RESEARCH_ROOT,
    DEFAULT_EXECUTION_GUARDRAILS_DIR,
    DEFAULT_LATEST_POINTER_DIR,
    DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
    DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
    DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    DEFAULT_POSITION_DRIFT_DIR,
    DEFAULT_SHADOW_SHORTLIST_DIR,
    DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY,
    DynamicV3ParameterResearchError,
    build_manual_execution_review_pack,
    run_execution_guardrails_check,
    run_portfolio_exposure_validation,
    run_position_drift_analysis,
    validate_execution_guardrails_artifact,
    validate_manual_portfolio_artifact,
    validate_position_drift_artifact,
    write_manual_portfolio_snapshot_artifact,
)
from ai_trading_system.etf_portfolio.owner_review_privacy import (
    ACCOUNT_NUMBER_RE,
    owner_notes_sensitive_issues,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

SCHEMA_VERSION = 1
PRODUCTION_EFFECT = "none"
REPORT_ROOT = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT

DEFAULT_REAL_SNAPSHOT_TEMPLATE_PATH = (
    PROJECT_ROOT
    / "config"
    / "etf_portfolio"
    / "dynamic_v3_rescue"
    / "current_portfolio_snapshot.real.template.yaml"
)
DEFAULT_REAL_SNAPSHOT_INTAKE_DIR = REPORT_ROOT / "real_snapshot_intake"
DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR = REPORT_ROOT / "real_snapshot_dry_run"
DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR = REPORT_ROOT / "real_execution_owner_review"
DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR = REPORT_ROOT / "real_snapshot_paper_action"
DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR = REPORT_ROOT / "weekly_real_snapshot_review"

OWNER_DECISIONS = {
    "pending",
    "monitor",
    "no_trade",
    "paper_adjustment_review_only",
    "reject_advisory",
    "needs_more_data",
    "defer",
}

SENSITIVE_KEY_PATTERNS = {
    "contains_account_number": (
        "account_number",
        "account_no",
        "broker_account_number",
        "brokerage_account_number",
    ),
    "contains_order_id": ("order_id", "broker_order_id", "trade_id", "execution_id"),
    "contains_tax_lot": ("tax_lot", "tax_lots", "cost_basis_lot"),
    "contains_personal_identifier": (
        "ssn",
        "passport",
        "national_id",
        "personal_identifier",
        "id_number",
    ),
}
FORBIDDEN_PATH_KEYS = {"broker_statement_path", "statement_path", "trade_confirmation_path"}
class RealSnapshotError(ValueError):
    """Raised when real manual snapshot dry-run artifacts are invalid."""


def real_snapshot_template_payload() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "snapshot": {
            "as_of": "YYYY-MM-DD",
            "source": "manual_owner_input",
            "broker_imported": False,
            "owner_reviewed": False,
            "base_currency": "USD",
            "total_equity": 0.0,
            "cash_symbol": "CASH",
            "redaction_status": "REDACTED",
        },
        "accounts": [
            {
                "account_id": "manual_primary",
                "account_type": "manual_snapshot",
                "currency": "USD",
                "total_equity": 0.0,
            }
        ],
        "cash": {
            "symbol": "CASH",
            "value": 0.0,
            "weight": 0.0,
            "currency": "USD",
        },
        "positions": [
            {
                "symbol": "QQQ",
                "asset_type": "ETF",
                "quantity": None,
                "market_price": None,
                "value": 0.0,
                "weight": 0.0,
                "currency": "USD",
                "account_id": "manual_primary",
            }
        ],
        "metadata": {
            "owner_notes": "",
            "last_manual_update": "YYYY-MM-DD",
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "contains_account_number": False,
            "contains_order_id": False,
            "contains_tax_lot": False,
        },
    }


def write_real_snapshot_template(
    path: Path = DEFAULT_REAL_SNAPSHOT_TEMPLATE_PATH,
    *,
    overwrite: bool = True,
) -> dict[str, Any]:
    if path.exists() and not overwrite:
        raise RealSnapshotError(f"template already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = real_snapshot_template_payload()
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_template",
        "template_path": str(path),
        "status": "PASS",
        "redaction_safe": True,
        "broker_imported": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }


def lint_real_snapshot_file(snapshot_path: Path) -> dict[str, Any]:
    raw = safe_load_yaml_path(snapshot_path)
    if not isinstance(raw, Mapping):
        return _redaction_check(
            snapshot_path=snapshot_path,
            payload={},
            blocking_issues=["snapshot_yaml_not_mapping"],
        )
    return _redaction_check(snapshot_path=snapshot_path, payload=raw)


def intake_real_snapshot(
    *,
    snapshot_path: Path,
    schema_config_path: Path = DEFAULT_MANUAL_PORTFOLIO_SCHEMA_CONFIG_PATH,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
    manual_snapshot_output_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    redaction = lint_real_snapshot_file(snapshot_path)
    snapshot_artifact: dict[str, Any] | None = None
    normalized: dict[str, Any] = {}
    snapshot_status = "FAIL"
    blocking = list(redaction["blocking_issues"])
    warnings = list(redaction["warnings"])
    if redaction["redaction_status"] != "FAIL":
        try:
            snapshot_artifact = write_manual_portfolio_snapshot_artifact(
                snapshot_path=snapshot_path,
                schema_config_path=schema_config_path,
                output_dir=manual_snapshot_output_dir,
                generated_at=generated,
            )
            normalized = dict(snapshot_artifact["normalized_portfolio"])
            snapshot_status = _text(snapshot_artifact["manifest"].get("status"), "FAIL")
        except DynamicV3ParameterResearchError as exc:
            blocking.append(f"manual_snapshot_validation_error:{exc}")
    if snapshot_status == "FAIL" and "manual_snapshot_status_fail" not in blocking:
        blocking.append("manual_snapshot_status_fail")

    status = "PASS" if not blocking else "FAIL"
    if status == "PASS" and redaction["redaction_status"] == "PASS_WITH_WARNINGS":
        status = "PASS_WITH_WARNINGS"
    snapshot_intake_id = _stable_id("real-snapshot-intake", snapshot_path, generated.isoformat())
    intake_dir = _unique_dir(output_dir / snapshot_intake_id)
    intake_dir.mkdir(parents=True, exist_ok=False)
    if snapshot_artifact:
        manual_snapshot_id = _text(snapshot_artifact["snapshot_id"])
        normalized["snapshot_intake_id"] = intake_dir.name
    else:
        manual_snapshot_id = ""
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_intake_manifest",
        "snapshot_intake_id": intake_dir.name,
        "snapshot_path": str(snapshot_path),
        "schema_config_path": str(schema_config_path),
        "manual_portfolio_snapshot_id": manual_snapshot_id,
        "generated_at": generated.isoformat(),
        "status": status,
        "snapshot_status": snapshot_status,
        "redaction_status": redaction["redaction_status"],
        "owner_reviewed": bool(normalized.get("owner_reviewed", False)),
        "manual_review_required": True,
        "redaction_check_path": str(intake_dir / "redaction_check.json"),
        "normalized_real_snapshot_path": str(intake_dir / "normalized_real_snapshot.json"),
        "real_snapshot_intake_report_path": str(intake_dir / "real_snapshot_intake_report.md"),
        "blocking_issues": blocking,
        "warnings": warnings,
        "broker_imported": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "owner_approval_required": True,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }
    _write_json(intake_dir / "real_snapshot_intake_manifest.json", manifest)
    _write_json(intake_dir / "redaction_check.json", redaction)
    _write_json(intake_dir / "normalized_real_snapshot.json", normalized)
    _write_text(
        intake_dir / "real_snapshot_intake_report.md",
        render_real_snapshot_intake_report(manifest, redaction, normalized),
    )
    _write_latest_pointer(
        "latest_real_snapshot_intake",
        intake_dir.name,
        intake_dir / "real_snapshot_intake_manifest.json",
    )
    return {
        "snapshot_intake_id": intake_dir.name,
        "intake_dir": intake_dir,
        "manifest": manifest,
        "redaction_check": redaction,
        "normalized_real_snapshot": normalized,
    }


def real_snapshot_report_payload(
    *,
    snapshot_intake_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
) -> dict[str, Any]:
    resolved_id = snapshot_intake_id or (
        _latest_pointer_artifact_id("latest_real_snapshot_intake") if latest else ""
    )
    if not resolved_id:
        raise RealSnapshotError("--snapshot-intake-id or --latest is required")
    intake_dir = output_dir / resolved_id
    return {
        **_read_json(intake_dir / "real_snapshot_intake_manifest.json"),
        "redaction_check": _read_optional_json(intake_dir / "redaction_check.json") or {},
        "normalized_real_snapshot": _read_optional_json(
            intake_dir / "normalized_real_snapshot.json"
        )
        or {},
        "intake_dir": str(intake_dir),
    }


def validate_real_snapshot(
    *,
    snapshot_intake_id: str,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
) -> dict[str, Any]:
    intake_dir = output_dir / snapshot_intake_id
    manifest = _read_optional_json(intake_dir / "real_snapshot_intake_manifest.json") or {}
    redaction = _read_optional_json(intake_dir / "redaction_check.json") or {}
    normalized = _read_optional_json(intake_dir / "normalized_real_snapshot.json") or {}
    checks = _required_file_checks(
        intake_dir,
        (
            "real_snapshot_intake_manifest.json",
            "redaction_check.json",
            "normalized_real_snapshot.json",
            "real_snapshot_intake_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "snapshot_intake_id_matches",
                manifest.get("snapshot_intake_id") == snapshot_intake_id,
                snapshot_intake_id,
            ),
            _check(
                "redaction_passed",
                redaction.get("redaction_status") in {"PASS", "PASS_WITH_WARNINGS"},
                _text(redaction.get("redaction_status")),
            ),
            _check(
                "snapshot_status_pass",
                manifest.get("snapshot_status") == "PASS",
                _text(manifest.get("snapshot_status")),
            ),
            _check(
                "manual_snapshot_link_present",
                bool(manifest.get("manual_portfolio_snapshot_id")),
                _text(manifest.get("manual_portfolio_snapshot_id")),
            ),
            _check(
                "broker_imported_false",
                manifest.get("broker_imported") is False
                and normalized.get("broker_imported") is False,
                "must remain manual owner input",
            ),
            _check("safety_locked", _payload_safe(manifest, normalized), "no broker/order"),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_real_snapshot_validation",
        checks,
        snapshot_intake_id,
    )


def run_real_snapshot_dry_run(
    *,
    snapshot_intake_id: str,
    shadow_shortlist_id: str,
    intake_dir: Path = DEFAULT_REAL_SNAPSHOT_INTAKE_DIR,
    manual_snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    exposure_dir: Path = DEFAULT_PORTFOLIO_EXPOSURE_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    manual_review_dir: Path = DEFAULT_MANUAL_EXECUTION_REVIEW_DIR,
    shadow_shortlist_dir: Path = DEFAULT_SHADOW_SHORTLIST_DIR,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    exposure_policy_config_path: Path = DEFAULT_PORTFOLIO_EXPOSURE_POLICY_CONFIG_PATH,
    position_config_path: Path = DEFAULT_POSITION_ADVISORY_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    intake = _read_json(intake_dir / snapshot_intake_id / "real_snapshot_intake_manifest.json")
    if intake.get("status") == "FAIL" or not intake.get("manual_portfolio_snapshot_id"):
        raise RealSnapshotError("real snapshot intake must pass before dry run")
    snapshot_id = _text(intake["manual_portfolio_snapshot_id"])
    exposure = run_portfolio_exposure_validation(
        snapshot_id=snapshot_id,
        snapshot_dir=manual_snapshot_dir,
        policy_config_path=exposure_policy_config_path,
        output_dir=exposure_dir,
        generated_at=generated,
    )
    drift = run_position_drift_analysis(
        snapshot_id=snapshot_id,
        shadow_shortlist_id=shadow_shortlist_id,
        snapshot_dir=manual_snapshot_dir,
        shadow_shortlist_dir=shadow_shortlist_dir,
        config_path=position_config_path,
        output_dir=drift_dir,
        generated_at=generated,
    )
    guardrail = run_execution_guardrails_check(
        drift_id=drift["drift_id"],
        exposure_id=exposure["exposure_id"],
        drift_dir=drift_dir,
        exposure_dir=exposure_dir,
        output_dir=guardrail_dir,
        generated_at=generated,
    )
    review = build_manual_execution_review_pack(
        snapshot_id=snapshot_id,
        exposure_id=exposure["exposure_id"],
        drift_id=drift["drift_id"],
        guardrail_id=guardrail["guardrail_id"],
        snapshot_dir=manual_snapshot_dir,
        exposure_dir=exposure_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
        output_dir=manual_review_dir,
        generated_at=generated,
    )
    dry_run_id = _stable_id(
        "real-snapshot-dry-run",
        snapshot_intake_id,
        shadow_shortlist_id,
        generated.isoformat(),
    )
    dry_dir = _unique_dir(output_dir / dry_run_id)
    dry_dir.mkdir(parents=True, exist_ok=False)
    links = {
        "snapshot_intake_id": snapshot_intake_id,
        "manual_portfolio_snapshot_id": snapshot_id,
        "exposure_id": exposure["exposure_id"],
        "drift_id": drift["drift_id"],
        "guardrail_id": guardrail["guardrail_id"],
        "manual_execution_review_id": review["manual_review_id"],
    }
    guardrail_summary = guardrail["guardrail_summary"]
    dry_summary = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_dry_run_summary",
        "dry_run_id": dry_dir.name,
        "snapshot_as_of": _text(intake.get("as_of"), ""),
        "snapshot_status": _dry_snapshot_status(intake),
        "exposure_status": _text(exposure["exposure_summary"].get("status"), "FAIL"),
        "drift_status": _text(drift["consensus_drift_summary"].get("drift_status"), "FAIL"),
        "guardrail_status": _guardrail_status(guardrail_summary),
        "manual_review_recommended_action": _text(
            review["manual_execution_decision"].get("recommended_action"),
            "manual_review",
        ),
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "owner_approval_required": True,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_dry_run_manifest",
        "dry_run_id": dry_dir.name,
        "generated_at": generated.isoformat(),
        "status": "PASS" if dry_summary["guardrail_status"] != "BLOCKED" else "PASS_WITH_WARNINGS",
        "shadow_shortlist_id": shadow_shortlist_id,
        "dry_run_artifact_links_path": str(dry_dir / "dry_run_artifact_links.json"),
        "real_snapshot_dry_run_summary_path": str(dry_dir / "real_snapshot_dry_run_summary.json"),
        "real_snapshot_dry_run_report_path": str(dry_dir / "real_snapshot_dry_run_report.md"),
        "reader_brief_section_path": str(dry_dir / "reader_brief_section.md"),
        **dry_summary,
    }
    _write_json(dry_dir / "real_snapshot_dry_run_manifest.json", manifest)
    _write_json(dry_dir / "dry_run_artifact_links.json", links)
    _write_json(dry_dir / "real_snapshot_dry_run_summary.json", dry_summary)
    _write_text(
        dry_dir / "real_snapshot_dry_run_report.md",
        render_real_snapshot_dry_run_report(manifest, links, dry_summary),
    )
    _write_text(dry_dir / "reader_brief_section.md", render_dry_run_reader_brief(dry_summary))
    _write_latest_pointer(
        "latest_real_snapshot_dry_run",
        dry_dir.name,
        dry_dir / "real_snapshot_dry_run_manifest.json",
    )
    return {
        "dry_run_id": dry_dir.name,
        "dry_run_dir": dry_dir,
        "manifest": manifest,
        "dry_run_artifact_links": links,
        "real_snapshot_dry_run_summary": dry_summary,
    }


def real_snapshot_dry_run_report_payload(
    *,
    dry_run_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
) -> dict[str, Any]:
    resolved_id = dry_run_id or (
        _latest_pointer_artifact_id("latest_real_snapshot_dry_run") if latest else ""
    )
    if not resolved_id:
        raise RealSnapshotError("--dry-run-id or --latest is required")
    dry_dir = output_dir / resolved_id
    return {
        **_read_json(dry_dir / "real_snapshot_dry_run_manifest.json"),
        "dry_run_artifact_links": _read_optional_json(dry_dir / "dry_run_artifact_links.json")
        or {},
        "real_snapshot_dry_run_summary": _read_optional_json(
            dry_dir / "real_snapshot_dry_run_summary.json"
        )
        or {},
        "dry_run_dir": str(dry_dir),
    }


def validate_real_snapshot_dry_run(
    *,
    dry_run_id: str,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
) -> dict[str, Any]:
    dry_dir = output_dir / dry_run_id
    manifest = _read_optional_json(dry_dir / "real_snapshot_dry_run_manifest.json") or {}
    summary = _read_optional_json(dry_dir / "real_snapshot_dry_run_summary.json") or {}
    links = _read_optional_json(dry_dir / "dry_run_artifact_links.json") or {}
    checks = _required_file_checks(
        dry_dir,
        (
            "real_snapshot_dry_run_manifest.json",
            "dry_run_artifact_links.json",
            "real_snapshot_dry_run_summary.json",
            "real_snapshot_dry_run_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "dry_run_id_matches",
                manifest.get("dry_run_id") == dry_run_id
                and summary.get("dry_run_id") == dry_run_id,
                dry_run_id,
            ),
            _check(
                "artifact_links_present",
                all(
                    links.get(key)
                    for key in (
                        "snapshot_intake_id",
                        "manual_portfolio_snapshot_id",
                        "exposure_id",
                        "drift_id",
                        "guardrail_id",
                        "manual_execution_review_id",
                    )
                ),
                "dry run links",
            ),
            _check(
                "recommended_action_valid",
                summary.get("manual_review_recommended_action") in {
                    "no_trade",
                    "monitor",
                    "manual_review",
                    "paper_adjustment_review_only",
                    "blocked",
                },
                _text(summary.get("manual_review_recommended_action")),
            ),
            _check(
                "order_ticket_not_generated",
                summary.get("order_ticket_generated") is False,
                "",
            ),
            _check("safety_locked", _payload_safe(manifest, summary), "no broker/order"),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_real_snapshot_dry_run_validation",
        checks,
        dry_run_id,
    )


def create_real_execution_owner_review(
    *,
    dry_run_id: str,
    dry_run_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    output_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    dry_payload = real_snapshot_dry_run_report_payload(
        dry_run_id=dry_run_id,
        output_dir=dry_run_dir,
    )
    links = _mapping(dry_payload.get("dry_run_artifact_links"))
    summary = _mapping(dry_payload.get("real_snapshot_dry_run_summary"))
    review_id = _stable_id("real-execution-owner-review", dry_run_id, generated.isoformat())
    review_dir = _unique_dir(output_dir / review_id)
    review_dir.mkdir(parents=True, exist_ok=False)
    decision = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_owner_execution_decision",
        "review_id": review_dir.name,
        "dry_run_id": dry_run_id,
        "manual_execution_review_id": _text(links.get("manual_execution_review_id")),
        "recommended_action": _text(
            summary.get("manual_review_recommended_action"),
            "manual_review",
        ),
        "owner_decision": "pending",
        "owner_notes": "",
        "order_ticket_generated": False,
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "production_effect": PRODUCTION_EFFECT,
        "created_at": generated.isoformat(),
        "updated_at": generated.isoformat(),
        **_safety(),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_execution_owner_review_manifest",
        "review_id": review_dir.name,
        "dry_run_id": dry_run_id,
        "manual_execution_review_id": decision["manual_execution_review_id"],
        "generated_at": generated.isoformat(),
        "status": "PENDING_OWNER_DECISION",
        "owner_execution_decision_path": str(review_dir / "owner_execution_decision.json"),
        "real_execution_owner_review_report_path": str(
            review_dir / "real_execution_owner_review_report.md"
        ),
        **decision,
    }
    _write_json(review_dir / "real_execution_owner_review_manifest.json", manifest)
    _write_json(review_dir / "owner_execution_decision.json", decision)
    _write_text(
        review_dir / "real_execution_owner_review_report.md",
        render_real_execution_owner_review_report(manifest, decision),
    )
    _write_latest_pointer(
        "latest_real_execution_owner_review",
        review_dir.name,
        review_dir / "real_execution_owner_review_manifest.json",
    )
    return {"review_id": review_dir.name, "review_dir": review_dir, "manifest": manifest}


def record_real_execution_owner_decision(
    *,
    review_id: str,
    decision: str,
    owner_notes: str = "",
    output_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    updated_at: datetime | None = None,
) -> dict[str, Any]:
    if decision not in OWNER_DECISIONS - {"pending"}:
        raise RealSnapshotError(f"unsupported owner decision: {decision}")
    sensitive_issues = owner_notes_sensitive_issues(owner_notes)
    if sensitive_issues:
        raise RealSnapshotError(
            "owner notes contain sensitive account data: " + ",".join(sensitive_issues)
        )
    updated = updated_at or datetime.now(UTC)
    review_dir = output_dir / review_id
    existing = _read_json(review_dir / "owner_execution_decision.json")
    existing["owner_decision"] = decision
    existing["owner_notes"] = owner_notes
    existing["updated_at"] = updated.isoformat()
    existing["order_ticket_generated"] = False
    existing["broker_action_allowed"] = False
    existing["broker_action_taken"] = False
    existing["production_effect"] = PRODUCTION_EFFECT
    manifest = _read_json(review_dir / "real_execution_owner_review_manifest.json")
    manifest.update(
        {
            "status": "PASS",
            "owner_decision": decision,
            "owner_notes": owner_notes,
            "updated_at": updated.isoformat(),
            "broker_action_allowed": False,
            "broker_action_taken": False,
            "order_ticket_generated": False,
            "production_effect": PRODUCTION_EFFECT,
        }
    )
    _write_json(review_dir / "owner_execution_decision.json", existing)
    _write_json(review_dir / "real_execution_owner_review_manifest.json", manifest)
    _write_text(
        review_dir / "real_execution_owner_review_report.md",
        render_real_execution_owner_review_report(manifest, existing),
    )
    _write_latest_pointer(
        "latest_real_execution_owner_review",
        review_id,
        review_dir / "real_execution_owner_review_manifest.json",
    )
    return {
        "review_id": review_id,
        "review_dir": review_dir,
        "manifest": manifest,
        "decision": existing,
    }


def real_execution_owner_review_report_payload(
    *,
    review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> dict[str, Any]:
    resolved_id = review_id or (
        _latest_pointer_artifact_id("latest_real_execution_owner_review") if latest else ""
    )
    if not resolved_id:
        raise RealSnapshotError("--review-id or --latest is required")
    review_dir = output_dir / resolved_id
    return {
        **_read_json(review_dir / "real_execution_owner_review_manifest.json"),
        "owner_execution_decision": _read_optional_json(
            review_dir / "owner_execution_decision.json"
        )
        or {},
        "review_dir": str(review_dir),
    }


def validate_real_execution_owner_review(
    *,
    review_id: str,
    output_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
) -> dict[str, Any]:
    review_dir = output_dir / review_id
    manifest = _read_optional_json(review_dir / "real_execution_owner_review_manifest.json") or {}
    decision = _read_optional_json(review_dir / "owner_execution_decision.json") or {}
    owner_note_issues = sorted(
        {
            *(
                f"manifest:{issue}"
                for issue in owner_notes_sensitive_issues(
                    _text(manifest.get("owner_notes"))
                )
            ),
            *(
                f"decision:{issue}"
                for issue in owner_notes_sensitive_issues(
                    _text(decision.get("owner_notes"))
                )
            ),
        }
    )
    checks = _required_file_checks(
        review_dir,
        (
            "real_execution_owner_review_manifest.json",
            "owner_execution_decision.json",
            "real_execution_owner_review_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "review_id_matches",
                manifest.get("review_id") == review_id and decision.get("review_id") == review_id,
                review_id,
            ),
            _check(
                "owner_decision_valid",
                decision.get("owner_decision") in OWNER_DECISIONS,
                _text(decision.get("owner_decision")),
            ),
            _check("broker_action_not_taken", decision.get("broker_action_taken") is False, ""),
            _check(
                "order_ticket_not_generated",
                decision.get("order_ticket_generated") is False,
                "",
            ),
            _check(
                "production_effect_none",
                decision.get("production_effect") == PRODUCTION_EFFECT,
                "",
            ),
            _check(
                "owner_notes_redaction_safe",
                not owner_note_issues,
                ",".join(owner_note_issues),
            ),
            _check("safety_locked", _payload_safe(manifest, decision), "no broker/order"),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_real_execution_owner_review_validation",
        checks,
        review_id,
    )


def apply_real_snapshot_paper_action(
    *,
    owner_review_id: str,
    owner_review_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    dry_run_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    manual_snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    owner_validation = validate_real_execution_owner_review(
        review_id=owner_review_id,
        output_dir=owner_review_dir,
    )
    if owner_validation.get("status") != "PASS":
        raise RealSnapshotError(
            f"owner review validation failed: {owner_validation.get('failed_check_count')} checks"
        )
    owner_payload = real_execution_owner_review_report_payload(
        review_id=owner_review_id,
        output_dir=owner_review_dir,
    )
    owner_decision = _mapping(owner_payload.get("owner_execution_decision"))
    dry_run_id = _text(owner_decision.get("dry_run_id"))
    decision = _text(owner_decision.get("owner_decision"), "pending")
    if owner_payload.get("status") != "PASS" or decision not in OWNER_DECISIONS - {"pending"}:
        raise RealSnapshotError("paper action requires a recorded non-pending owner decision")
    if (
        owner_payload.get("review_id") != owner_review_id
        or owner_decision.get("review_id") != owner_review_id
        or owner_payload.get("dry_run_id") != dry_run_id
    ):
        raise RealSnapshotError("owner review lineage does not match requested review")
    dry_validation = validate_real_snapshot_dry_run(
        dry_run_id=dry_run_id,
        output_dir=dry_run_dir,
    )
    if dry_validation.get("status") != "PASS":
        raise RealSnapshotError(
            f"dry run validation failed: {dry_validation.get('failed_check_count')} checks"
        )
    dry_payload = real_snapshot_dry_run_report_payload(
        dry_run_id=dry_run_id,
        output_dir=dry_run_dir,
    )
    links = _mapping(dry_payload.get("dry_run_artifact_links"))
    snapshot_id = _text(links.get("manual_portfolio_snapshot_id"))
    drift_id = _text(links.get("drift_id"))
    guardrail_id = _text(links.get("guardrail_id"))
    source_validations = {
        "manual_snapshot": validate_manual_portfolio_artifact(
            snapshot_id=snapshot_id,
            output_dir=manual_snapshot_dir,
        ),
        "position_drift": validate_position_drift_artifact(
            drift_id=drift_id,
            output_dir=drift_dir,
        ),
        "execution_guardrails": validate_execution_guardrails_artifact(
            guardrail_id=guardrail_id,
            output_dir=guardrail_dir,
        ),
    }
    failed_sources = sorted(
        name for name, payload in source_validations.items() if payload.get("status") != "PASS"
    )
    if failed_sources:
        raise RealSnapshotError(
            "paper action source validation failed: " + ",".join(failed_sources)
        )
    source_paths = _paper_action_source_paths(
        owner_review_id=owner_review_id,
        dry_run_id=dry_run_id,
        snapshot_id=snapshot_id,
        drift_id=drift_id,
        guardrail_id=guardrail_id,
        owner_review_dir=owner_review_dir,
        dry_run_dir=dry_run_dir,
        manual_snapshot_dir=manual_snapshot_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
    )
    normalized = _read_json(source_paths["normalized_portfolio"])
    drift_manifest = _read_json(source_paths["position_drift_manifest"])
    drift_summary = _read_json(source_paths["consensus_drift_summary"])
    guardrail_manifest = _read_json(source_paths["guardrail_manifest"])
    guardrail_checks = _read_jsonl(source_paths["proposed_adjustment_checks"])
    if (
        normalized.get("snapshot_id") != snapshot_id
        or drift_manifest.get("snapshot_id") != snapshot_id
        or drift_manifest.get("drift_id") != drift_id
        or guardrail_manifest.get("snapshot_id") != snapshot_id
        or guardrail_manifest.get("drift_id") != drift_id
        or guardrail_manifest.get("guardrail_id") != guardrail_id
    ):
        raise RealSnapshotError("paper action source lineage mismatch")
    before_weights = _float_mapping(normalized.get("weights"))
    proposed_deltas = _float_mapping(drift_summary.get("consensus_deltas"))
    action_type = "paper_only" if decision == "paper_adjustment_review_only" else "no_action"
    applied = (
        _self_financing_paper_deltas(guardrail_checks)
        if action_type == "paper_only"
        else {symbol: 0.0 for symbol in sorted(set(before_weights) | set(proposed_deltas))}
    )
    after_weights = _apply_deltas(before_weights, applied)
    paper_action_id = _stable_id(
        "real-snapshot-paper-action",
        owner_review_id,
        generated.isoformat(),
    )
    action_dir = _unique_dir(output_dir / paper_action_id)
    action_dir.mkdir(parents=True, exist_ok=False)
    action = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_action_from_real_snapshot",
        "paper_action_id": action_dir.name,
        "owner_review_id": owner_review_id,
        "dry_run_id": dry_run_id,
        "snapshot_id": snapshot_id,
        "drift_id": drift_id,
        "owner_decision": decision,
        "action_type": action_type,
        "before_weights": before_weights,
        "proposed_deltas": proposed_deltas,
        "applied_paper_deltas": applied,
        "after_weights": after_weights,
        "guardrail_id": guardrail_id,
        "source_artifact_paths": {key: str(path) for key, path in source_paths.items()},
        "source_artifact_checksums": {
            key: _file_sha256(path) for key, path in source_paths.items()
        },
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }
    paper_state = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_paper_state_after_real_snapshot_action",
        "paper_action_id": action_dir.name,
        "source_snapshot_id": snapshot_id,
        "weights": after_weights,
        "weight_sum": round(sum(after_weights.values()), 6),
        "real_snapshot_mutated": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_paper_action_manifest",
        "paper_action_id": action_dir.name,
        "owner_review_id": owner_review_id,
        "dry_run_id": dry_run_id,
        "generated_at": generated.isoformat(),
        "status": "PASS",
        "action_type": action_type,
        "paper_action_from_real_snapshot_path": str(
            action_dir / "paper_action_from_real_snapshot.json"
        ),
        "paper_state_after_action_path": str(action_dir / "paper_state_after_action.json"),
        "real_snapshot_paper_action_report_path": str(
            action_dir / "real_snapshot_paper_action_report.md"
        ),
        **action,
    }
    _write_json(action_dir / "real_snapshot_paper_action_manifest.json", manifest)
    _write_json(action_dir / "paper_action_from_real_snapshot.json", action)
    _write_json(action_dir / "paper_state_after_action.json", paper_state)
    _write_text(
        action_dir / "real_snapshot_paper_action_report.md",
        render_real_snapshot_paper_action_report(manifest, action),
    )
    _write_latest_pointer(
        "latest_real_snapshot_paper_action",
        action_dir.name,
        action_dir / "real_snapshot_paper_action_manifest.json",
    )
    return {
        "paper_action_id": action_dir.name,
        "paper_action_dir": action_dir,
        "manifest": manifest,
        "paper_action_from_real_snapshot": action,
        "paper_state_after_action": paper_state,
    }


def real_snapshot_paper_action_report_payload(
    *,
    paper_action_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
) -> dict[str, Any]:
    resolved_id = paper_action_id or (
        _latest_pointer_artifact_id("latest_real_snapshot_paper_action") if latest else ""
    )
    if not resolved_id:
        raise RealSnapshotError("--paper-action-id or --latest is required")
    action_dir = output_dir / resolved_id
    return {
        **_read_json(action_dir / "real_snapshot_paper_action_manifest.json"),
        "paper_action_from_real_snapshot": _read_optional_json(
            action_dir / "paper_action_from_real_snapshot.json"
        )
        or {},
        "paper_state_after_action": _read_optional_json(
            action_dir / "paper_state_after_action.json"
        )
        or {},
        "paper_action_dir": str(action_dir),
    }


def validate_real_snapshot_paper_action(
    *,
    paper_action_id: str,
    output_dir: Path = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
    owner_review_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    dry_run_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    manual_snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> dict[str, Any]:
    action_dir = output_dir / paper_action_id
    manifest = _read_optional_json(action_dir / "real_snapshot_paper_action_manifest.json") or {}
    action = _read_optional_json(action_dir / "paper_action_from_real_snapshot.json") or {}
    paper_state = _read_optional_json(action_dir / "paper_state_after_action.json") or {}
    owner_review_id = _text(action.get("owner_review_id"))
    dry_run_id = _text(action.get("dry_run_id"))
    snapshot_id = _text(action.get("snapshot_id"))
    drift_id = _text(action.get("drift_id"))
    guardrail_id = _text(action.get("guardrail_id"))
    expected_source_paths = _paper_action_source_paths(
        owner_review_id=owner_review_id,
        dry_run_id=dry_run_id,
        snapshot_id=snapshot_id,
        drift_id=drift_id,
        guardrail_id=guardrail_id,
        owner_review_dir=owner_review_dir,
        dry_run_dir=dry_run_dir,
        manual_snapshot_dir=manual_snapshot_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
    )
    recorded_source_paths = _mapping(action.get("source_artifact_paths"))
    recorded_source_checksums = _mapping(action.get("source_artifact_checksums"))
    source_files_present = all(path.is_file() for path in expected_source_paths.values())
    source_checksums_match = source_files_present and all(
        recorded_source_checksums.get(key) == _file_sha256(path)
        for key, path in expected_source_paths.items()
    )
    owner_manifest = _read_optional_json(expected_source_paths["owner_review_manifest"]) or {}
    owner_decision = _read_optional_json(expected_source_paths["owner_execution_decision"]) or {}
    dry_links = _read_optional_json(expected_source_paths["dry_run_artifact_links"]) or {}
    normalized = _read_optional_json(expected_source_paths["normalized_portfolio"]) or {}
    drift_manifest = _read_optional_json(expected_source_paths["position_drift_manifest"]) or {}
    drift_summary = _read_optional_json(expected_source_paths["consensus_drift_summary"]) or {}
    guardrail_manifest = _read_optional_json(expected_source_paths["guardrail_manifest"]) or {}
    guardrail_checks = _read_jsonl(expected_source_paths["proposed_adjustment_checks"])
    expected_decision = _text(owner_decision.get("owner_decision"), "pending")
    expected_action_type = (
        "paper_only" if expected_decision == "paper_adjustment_review_only" else "no_action"
    )
    expected_before = _float_mapping(normalized.get("weights"))
    expected_proposed = _float_mapping(drift_summary.get("consensus_deltas"))
    expected_applied = (
        _self_financing_paper_deltas(guardrail_checks)
        if expected_action_type == "paper_only"
        else {
            symbol: 0.0
            for symbol in sorted(set(expected_before) | set(expected_proposed))
        }
    )
    expected_after = _apply_deltas(expected_before, expected_applied)
    source_validations = {
        "owner_review": validate_real_execution_owner_review(
            review_id=owner_review_id,
            output_dir=owner_review_dir,
        ),
        "dry_run": validate_real_snapshot_dry_run(
            dry_run_id=dry_run_id,
            output_dir=dry_run_dir,
        ),
        "manual_snapshot": validate_manual_portfolio_artifact(
            snapshot_id=snapshot_id,
            output_dir=manual_snapshot_dir,
        ),
        "position_drift": validate_position_drift_artifact(
            drift_id=drift_id,
            output_dir=drift_dir,
        ),
        "execution_guardrails": validate_execution_guardrails_artifact(
            guardrail_id=guardrail_id,
            output_dir=guardrail_dir,
        ),
    }
    sources_validate = all(
        payload.get("status") == "PASS" for payload in source_validations.values()
    )
    source_lineage_matches = (
        owner_manifest.get("review_id") == owner_review_id
        and owner_decision.get("review_id") == owner_review_id
        and owner_manifest.get("dry_run_id") == dry_run_id
        and owner_decision.get("dry_run_id") == dry_run_id
        and dry_links.get("manual_portfolio_snapshot_id") == snapshot_id
        and dry_links.get("drift_id") == drift_id
        and dry_links.get("guardrail_id") == guardrail_id
        and normalized.get("snapshot_id") == snapshot_id
        and drift_manifest.get("snapshot_id") == snapshot_id
        and drift_manifest.get("drift_id") == drift_id
        and guardrail_manifest.get("snapshot_id") == snapshot_id
        and guardrail_manifest.get("drift_id") == drift_id
        and guardrail_manifest.get("guardrail_id") == guardrail_id
    )
    action_content_derived = (
        action.get("owner_decision") == expected_decision
        and action.get("action_type") == expected_action_type
        and _float_mapping(action.get("before_weights")) == expected_before
        and _float_mapping(action.get("proposed_deltas")) == expected_proposed
        and _float_mapping(action.get("applied_paper_deltas")) == expected_applied
        and _float_mapping(action.get("after_weights")) == expected_after
    )
    manifest_action_consistent = all(
        manifest.get(key) == action.get(key)
        for key in (
            "paper_action_id",
            "owner_review_id",
            "dry_run_id",
            "snapshot_id",
            "drift_id",
            "guardrail_id",
            "owner_decision",
            "action_type",
            "before_weights",
            "proposed_deltas",
            "applied_paper_deltas",
            "after_weights",
            "source_artifact_paths",
            "source_artifact_checksums",
        )
    )
    checks = _required_file_checks(
        action_dir,
        (
            "real_snapshot_paper_action_manifest.json",
            "paper_action_from_real_snapshot.json",
            "paper_state_after_action.json",
            "real_snapshot_paper_action_report.md",
        ),
    )
    checks.extend(
        [
            _check(
                "paper_action_id_matches",
                manifest.get("paper_action_id") == paper_action_id
                and action.get("paper_action_id") == paper_action_id
                and paper_state.get("paper_action_id") == paper_action_id,
                paper_action_id,
            ),
            _check(
                "owner_decision_final",
                owner_manifest.get("status") == "PASS"
                and expected_decision in OWNER_DECISIONS - {"pending"},
                expected_decision,
            ),
            _check("source_artifacts_validate", sources_validate, "all sources must PASS"),
            _check("source_lineage_matches", source_lineage_matches, "source ids"),
            _check(
                "source_paths_match",
                recorded_source_paths
                == {key: str(path) for key, path in expected_source_paths.items()}
                and manifest.get("source_artifact_paths") == action.get("source_artifact_paths"),
                "canonical source paths",
            ),
            _check("source_files_present", source_files_present, "source artifacts"),
            _check("source_checksums_match", source_checksums_match, "source artifacts"),
            _check(
                "action_type_valid",
                action.get("action_type") in {"paper_only", "no_action"},
                "",
            ),
            _check("action_content_derived", action_content_derived, "source recomputation"),
            _check(
                "manifest_action_consistent",
                manifest_action_consistent,
                "manifest/action payload",
            ),
            _check(
                "paper_state_weight_sum",
                abs(_safe_float(paper_state.get("weight_sum")) - 1.0) <= 0.0001,
                _text(paper_state.get("weight_sum")),
            ),
            _check(
                "paper_state_content_derived",
                paper_state.get("source_snapshot_id") == snapshot_id
                and _float_mapping(paper_state.get("weights")) == expected_after
                and paper_state.get("weight_sum") == round(sum(expected_after.values()), 6),
                "after weights projection",
            ),
            _check(
                "real_snapshot_not_mutated",
                paper_state.get("real_snapshot_mutated") is False,
                "",
            ),
            _check("order_ticket_not_generated", action.get("order_ticket_generated") is False, ""),
            _check("broker_action_not_taken", action.get("broker_action_taken") is False, ""),
            _check(
                "safety_locked",
                _payload_safe(manifest, action, paper_state),
                "no broker/order",
            ),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_real_snapshot_paper_action_validation",
        checks,
        paper_action_id,
    )


def run_weekly_real_snapshot_review(
    *,
    week_ending: date,
    dry_run_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    owner_review_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    paper_action_dir: Path = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
    manual_snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
    output_dir: Path = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    dry_run, owner, paper, selection_issues = _weekly_source_selection(
        week_ending=week_ending,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
    )
    inventory, inventory_issues = _owner_decision_inventory_at_or_before(
        owner_review_dir=owner_review_dir,
        week_ending=week_ending,
    )
    source_validation = _weekly_source_validation(
        dry_run=dry_run,
        owner=owner,
        paper=paper,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
        manual_snapshot_dir=manual_snapshot_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
    )
    source_failures = sorted(
        name for name, payload in source_validation.items() if payload.get("status") != "PASS"
    )
    blocking_issues = [*selection_issues, *inventory_issues]
    if source_failures:
        blocking_issues.append("source_validation_failed:" + ",".join(source_failures))
    if blocking_issues:
        raise RealSnapshotError(
            "weekly real snapshot source selection failed: " + ";".join(blocking_issues)
        )
    weekly_id = _stable_id(
        "weekly-real-snapshot-review",
        week_ending.isoformat(),
        generated.isoformat(),
    )
    weekly_dir = _unique_dir(output_dir / weekly_id)
    weekly_dir.mkdir(parents=True, exist_ok=False)
    summary = _weekly_summary_payload(
        weekly_real_review_id=weekly_dir.name,
        week_ending=week_ending,
        dry_run=dry_run,
        owner=owner,
        paper=paper,
    )
    decision_summary = _owner_decision_summary_from_inventory(inventory)
    source_paths = _weekly_source_paths(
        dry_run=dry_run,
        owner=owner,
        paper=paper,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
    )
    source_checksums = {key: _file_sha256(path) for key, path in source_paths.items()}
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_real_snapshot_review_manifest",
        "weekly_real_review_id": weekly_dir.name,
        "week_ending": week_ending.isoformat(),
        "generated_at": generated.isoformat(),
        "status": "PASS" if summary["chain_status"] == "COMPLETE" else "PASS_WITH_WARNINGS",
        "source_artifact_paths": {key: str(path) for key, path in source_paths.items()},
        "source_artifact_checksums": source_checksums,
        "weekly_real_snapshot_summary_path": str(weekly_dir / "weekly_real_snapshot_summary.json"),
        "weekly_owner_decision_inventory_path": str(
            weekly_dir / "weekly_owner_decision_inventory.json"
        ),
        "weekly_owner_decision_summary_path": str(
            weekly_dir / "weekly_owner_decision_summary.json"
        ),
        "weekly_real_snapshot_review_report_path": str(
            weekly_dir / "weekly_real_snapshot_review_report.md"
        ),
        "reader_brief_section_path": str(weekly_dir / "reader_brief_section.md"),
        **summary,
    }
    _write_json(weekly_dir / "weekly_real_snapshot_review_manifest.json", manifest)
    _write_json(weekly_dir / "weekly_real_snapshot_summary.json", summary)
    _write_json(weekly_dir / "weekly_owner_decision_inventory.json", inventory)
    _write_json(weekly_dir / "weekly_owner_decision_summary.json", decision_summary)
    _write_text(
        weekly_dir / "weekly_real_snapshot_review_report.md",
        render_weekly_real_snapshot_review_report(manifest, summary, decision_summary),
    )
    _write_text(weekly_dir / "reader_brief_section.md", render_weekly_real_reader_brief(summary))
    _write_latest_pointer(
        "latest_weekly_real_snapshot_review",
        weekly_dir.name,
        weekly_dir / "weekly_real_snapshot_review_manifest.json",
    )
    return {
        "weekly_real_review_id": weekly_dir.name,
        "weekly_real_review_dir": weekly_dir,
        "manifest": manifest,
        "weekly_real_snapshot_summary": summary,
        "weekly_owner_decision_inventory": inventory,
        "weekly_owner_decision_summary": decision_summary,
    }


def weekly_real_snapshot_review_report_payload(
    *,
    weekly_real_review_id: str | None = None,
    latest: bool = False,
    output_dir: Path = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
) -> dict[str, Any]:
    resolved_id = weekly_real_review_id or (
        _latest_pointer_artifact_id("latest_weekly_real_snapshot_review") if latest else ""
    )
    if not resolved_id:
        raise RealSnapshotError("--weekly-real-review-id or --latest is required")
    weekly_dir = output_dir / resolved_id
    return {
        **_read_json(weekly_dir / "weekly_real_snapshot_review_manifest.json"),
        "weekly_real_snapshot_summary": _read_optional_json(
            weekly_dir / "weekly_real_snapshot_summary.json"
        )
        or {},
        "weekly_owner_decision_inventory": _read_optional_json(
            weekly_dir / "weekly_owner_decision_inventory.json"
        )
        or {},
        "weekly_owner_decision_summary": _read_optional_json(
            weekly_dir / "weekly_owner_decision_summary.json"
        )
        or {},
        "weekly_real_review_dir": str(weekly_dir),
    }


def validate_weekly_real_snapshot_review(
    *,
    weekly_real_review_id: str,
    output_dir: Path = DEFAULT_WEEKLY_REAL_SNAPSHOT_REVIEW_DIR,
    dry_run_dir: Path = DEFAULT_REAL_SNAPSHOT_DRY_RUN_DIR,
    owner_review_dir: Path = DEFAULT_REAL_EXECUTION_OWNER_REVIEW_DIR,
    paper_action_dir: Path = DEFAULT_REAL_SNAPSHOT_PAPER_ACTION_DIR,
    manual_snapshot_dir: Path = DEFAULT_MANUAL_PORTFOLIO_SNAPSHOT_DIR,
    drift_dir: Path = DEFAULT_POSITION_DRIFT_DIR,
    guardrail_dir: Path = DEFAULT_EXECUTION_GUARDRAILS_DIR,
) -> dict[str, Any]:
    weekly_dir = output_dir / weekly_real_review_id
    manifest = _read_optional_json(weekly_dir / "weekly_real_snapshot_review_manifest.json") or {}
    summary = _read_optional_json(weekly_dir / "weekly_real_snapshot_summary.json") or {}
    inventory = _read_optional_json(weekly_dir / "weekly_owner_decision_inventory.json") or {}
    decision_summary = _read_optional_json(weekly_dir / "weekly_owner_decision_summary.json") or {}
    try:
        week_ending = date.fromisoformat(_text(summary.get("week_ending")))
        week_ending_valid = True
    except ValueError:
        week_ending = date.min
        week_ending_valid = False
    dry_run, owner, paper, selection_issues = _weekly_source_selection(
        week_ending=week_ending,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
    )
    expected_inventory, inventory_issues = _owner_decision_inventory_at_or_before(
        owner_review_dir=owner_review_dir,
        week_ending=week_ending,
    )
    expected_decision_summary = _owner_decision_summary_from_inventory(expected_inventory)
    expected_summary = _weekly_summary_payload(
        weekly_real_review_id=weekly_real_review_id,
        week_ending=week_ending,
        dry_run=dry_run,
        owner=owner,
        paper=paper,
    )
    expected_source_paths = _weekly_source_paths(
        dry_run=dry_run,
        owner=owner,
        paper=paper,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
    )
    recorded_source_paths = _mapping(manifest.get("source_artifact_paths"))
    recorded_source_checksums = _mapping(manifest.get("source_artifact_checksums"))
    source_files_present = all(path.is_file() for path in expected_source_paths.values())
    source_checksums_match = source_files_present and all(
        recorded_source_checksums.get(key) == _file_sha256(path)
        for key, path in expected_source_paths.items()
    )
    source_validation = _weekly_source_validation(
        dry_run=dry_run,
        owner=owner,
        paper=paper,
        dry_run_dir=dry_run_dir,
        owner_review_dir=owner_review_dir,
        paper_action_dir=paper_action_dir,
        manual_snapshot_dir=manual_snapshot_dir,
        drift_dir=drift_dir,
        guardrail_dir=guardrail_dir,
    )
    sources_validate = all(
        payload.get("status") == "PASS" for payload in source_validation.values()
    )
    expected_status = (
        "PASS" if expected_summary["chain_status"] == "COMPLETE" else "PASS_WITH_WARNINGS"
    )
    manifest_summary_consistent = all(
        manifest.get(key) == value for key, value in summary.items()
    )
    report_path = weekly_dir / "weekly_real_snapshot_review_report.md"
    reader_path = weekly_dir / "reader_brief_section.md"
    rendered_report_matches = report_path.exists() and report_path.read_text(
        encoding="utf-8"
    ) == render_weekly_real_snapshot_review_report(manifest, summary, decision_summary)
    rendered_reader_matches = reader_path.exists() and reader_path.read_text(
        encoding="utf-8"
    ) == render_weekly_real_reader_brief(summary)
    checks = _required_file_checks(
        weekly_dir,
        (
            "weekly_real_snapshot_review_manifest.json",
            "weekly_real_snapshot_summary.json",
            "weekly_owner_decision_inventory.json",
            "weekly_owner_decision_summary.json",
            "weekly_real_snapshot_review_report.md",
            "reader_brief_section.md",
        ),
    )
    checks.extend(
        [
            _check(
                "weekly_real_review_id_matches",
                manifest.get("weekly_real_review_id") == weekly_real_review_id
                and summary.get("weekly_real_review_id") == weekly_real_review_id,
                weekly_real_review_id,
            ),
            _check("week_ending_valid", week_ending_valid, _text(summary.get("week_ending"))),
            _check(
                "source_selection_unambiguous",
                not selection_issues and not inventory_issues,
                ";".join([*selection_issues, *inventory_issues]),
            ),
            _check("source_artifacts_validate", sources_validate, "selected source chain"),
            _check(
                "source_paths_match",
                recorded_source_paths
                == {key: str(path) for key, path in expected_source_paths.items()},
                "selected source chain",
            ),
            _check("source_files_present", source_files_present, "selected source chain"),
            _check("source_checksums_match", source_checksums_match, "selected source chain"),
            _check(
                "owner_decision_inventory_content_derived",
                inventory == expected_inventory,
                "owner review inventory as of week ending",
            ),
            _check(
                "owner_decision_summary_present",
                all(
                    f"{decision}_count" in decision_summary
                    for decision in OWNER_DECISIONS - {"pending"}
                )
                and "pending_reviews" in decision_summary,
                "owner decision counts",
            ),
            _check(
                "owner_decision_summary_content_derived",
                decision_summary == expected_decision_summary,
                "owner decision inventory counts",
            ),
            _check(
                "weekly_summary_content_derived",
                summary == expected_summary,
                "selected source chain",
            ),
            _check(
                "manifest_summary_consistent",
                manifest_summary_consistent
                and manifest.get("status") == expected_status,
                "manifest/summary",
            ),
            _check("rendered_report_matches", rendered_report_matches, str(report_path)),
            _check("rendered_reader_matches", rendered_reader_matches, str(reader_path)),
            _check("broker_action_not_taken", summary.get("broker_action_taken") is False, ""),
            _check(
                "order_ticket_not_generated",
                summary.get("order_ticket_generated") is False,
                "",
            ),
            _check("next_action_present", bool(summary.get("next_action")), ""),
            _check("safety_locked", _payload_safe(manifest, summary), "no broker/order"),
        ]
    )
    return _validation_payload(
        "etf_dynamic_v3_weekly_real_snapshot_review_validation",
        checks,
        weekly_real_review_id,
    )


def render_real_snapshot_intake_report(
    manifest: Mapping[str, Any],
    redaction: Mapping[str, Any],
    normalized: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Real Manual Snapshot Intake {manifest.get('snapshot_intake_id')}",
            "",
            f"- snapshot_status: {manifest.get('snapshot_status')}",
            f"- redaction_status: {redaction.get('redaction_status')}",
            f"- dry_run_usable: {manifest.get('status') in {'PASS', 'PASS_WITH_WARNINGS'}}",
            f"- broker_imported: {str(manifest.get('broker_imported')).lower()}",
            f"- owner_reviewed: {str(manifest.get('owner_reviewed')).lower()}",
            f"- total_equity: {normalized.get('total_equity', 'MISSING')}",
            f"- weight_sum: {normalized.get('weight_sum', 'MISSING')}",
            f"- value_sum: {normalized.get('value_sum', 'MISSING')}",
            f"- blocking_issues: {manifest.get('blocking_issues', [])}",
            f"- warnings: {manifest.get('warnings', [])}",
            "",
            "Broker action is forbidden; this is manual owner input, not broker import.",
            "",
        ]
    )


def render_real_snapshot_dry_run_report(
    manifest: Mapping[str, Any],
    links: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Real Snapshot Advisory Dry Run {manifest.get('dry_run_id')}",
            "",
            f"- snapshot_intake_id: {links.get('snapshot_intake_id')}",
            f"- manual_portfolio_snapshot_id: {links.get('manual_portfolio_snapshot_id')}",
            f"- exposure_status: {summary.get('exposure_status')}",
            f"- drift_status: {summary.get('drift_status')}",
            f"- guardrail_status: {summary.get('guardrail_status')}",
            f"- recommended_action: {summary.get('manual_review_recommended_action')}",
            f"- order_ticket_generated: {str(summary.get('order_ticket_generated')).lower()}",
            f"- broker_action_allowed: {str(summary.get('broker_action_allowed')).lower()}",
            "",
            "This dry run creates advisory evidence only and never generates order tickets.",
            "",
        ]
    )


def render_dry_run_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Real Snapshot Advisory Dry Run",
            "",
            f"- dry_run_id: {summary.get('dry_run_id')}",
            f"- snapshot_status: {summary.get('snapshot_status')}",
            f"- exposure_status: {summary.get('exposure_status')}",
            f"- drift_status: {summary.get('drift_status')}",
            f"- guardrail_status: {summary.get('guardrail_status')}",
            f"- recommended_action: {summary.get('manual_review_recommended_action')}",
            "- broker_action_allowed: false",
            "- order_ticket_generated: false",
            "",
        ]
    )


def render_real_execution_owner_review_report(
    manifest: Mapping[str, Any],
    decision: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Real Execution Owner Review {manifest.get('review_id')}",
            "",
            f"- dry_run_id: {decision.get('dry_run_id')}",
            f"- recommended_action: {decision.get('recommended_action')}",
            f"- owner_decision: {decision.get('owner_decision')}",
            f"- broker_action_taken: {str(decision.get('broker_action_taken')).lower()}",
            f"- order_ticket_generated: {str(decision.get('order_ticket_generated')).lower()}",
            f"- production_effect: {decision.get('production_effect')}",
            "",
        ]
    )


def render_real_snapshot_paper_action_report(
    manifest: Mapping[str, Any],
    action: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Real Snapshot Paper Action {manifest.get('paper_action_id')}",
            "",
            f"- owner_review_id: {action.get('owner_review_id')}",
            f"- owner_decision: {action.get('owner_decision')}",
            f"- action_type: {action.get('action_type')}",
            f"- broker_action_taken: {str(action.get('broker_action_taken')).lower()}",
            f"- order_ticket_generated: {str(action.get('order_ticket_generated')).lower()}",
            "",
        ]
    )


def render_weekly_real_snapshot_review_report(
    manifest: Mapping[str, Any],
    summary: Mapping[str, Any],
    decision_summary: Mapping[str, Any],
) -> str:
    return "\n".join(
        [
            f"# Weekly Real Snapshot Advisory Review {manifest.get('weekly_real_review_id')}",
            "",
            f"- week_ending: {summary.get('week_ending')}",
            f"- chain_status: {summary.get('chain_status')}",
            f"- latest_dry_run_id: {summary.get('latest_dry_run_id')}",
            f"- latest_owner_review_id: {summary.get('latest_owner_review_id')}",
            f"- latest_paper_action_id: {summary.get('latest_paper_action_id')}",
            f"- snapshot_status: {summary.get('snapshot_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- owner_decision: {summary.get('owner_decision')}",
            f"- paper_action_taken: {str(summary.get('paper_action_taken')).lower()}",
            f"- broker_action_taken: {str(summary.get('broker_action_taken')).lower()}",
            f"- next_action: {summary.get('next_action')}",
            "",
            f"- pending_reviews: {decision_summary.get('pending_reviews', 0)}",
            f"- monitor_count: {decision_summary.get('monitor_count', 0)}",
            f"- no_trade_count: {decision_summary.get('no_trade_count', 0)}",
            f"- paper_adjustment_review_only_count: "
            f"{decision_summary.get('paper_adjustment_review_only_count', 0)}",
            "",
        ]
    )


def render_weekly_real_reader_brief(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "## Dynamic Rescue Real Snapshot Advisory Review",
            "",
            f"- chain_status: {summary.get('chain_status')}",
            f"- snapshot_status: {summary.get('snapshot_status')}",
            f"- recommended_action: {summary.get('recommended_action')}",
            f"- owner_decision: {summary.get('owner_decision')}",
            f"- paper_action_taken: {str(summary.get('paper_action_taken')).lower()}",
            f"- broker_action_taken: {str(summary.get('broker_action_taken')).lower()}",
            f"- next_action: {summary.get('next_action')}",
            "",
        ]
    )


def _redaction_check(
    *,
    snapshot_path: Path,
    payload: Mapping[str, Any],
    blocking_issues: Sequence[str] = (),
) -> dict[str, Any]:
    found = {
        "contains_account_number": False,
        "contains_order_id": False,
        "contains_tax_lot": False,
        "contains_personal_identifier": False,
    }
    warnings: list[str] = []
    blocking = list(blocking_issues)
    for path, key, value in _walk_payload(payload):
        lowered_key = key.lower()
        for flag, patterns in SENSITIVE_KEY_PATTERNS.items():
            if lowered_key in patterns:
                found[flag] = True
                blocking.append(f"sensitive_key:{path}")
        if lowered_key in FORBIDDEN_PATH_KEYS:
            blocking.append(f"forbidden_statement_path:{path}")
        if (
            lowered_key == "account_id"
            and isinstance(value, str)
            and ACCOUNT_NUMBER_RE.search(value)
        ):
            found["contains_account_number"] = True
            blocking.append(f"account_id_looks_like_account_number:{path}")
        if isinstance(value, str) and ("statement" in lowered_key or "order" in lowered_key):
            warnings.append(f"review_text_value:{path}")
    metadata = _mapping(payload.get("metadata"))
    for flag in found:
        if metadata.get(flag) is True:
            found[flag] = True
            blocking.append(f"metadata_flag_true:{flag}")
    snapshot = _mapping(payload.get("snapshot"))
    if snapshot.get("broker_imported") is not False:
        blocking.append("broker_imported_must_be_false")
    if _mapping(payload.get("metadata")).get("broker_action_taken") is not False:
        blocking.append("broker_action_taken_must_be_false")
    status = "FAIL" if blocking else "PASS_WITH_WARNINGS" if warnings else "PASS"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_real_snapshot_redaction_check",
        "snapshot_path": str(snapshot_path),
        "redaction_status": status,
        **found,
        "warnings": sorted(set(warnings)),
        "blocking_issues": sorted(set(blocking)),
        "broker_imported": (
            False
            if snapshot.get("broker_imported") is False
            else snapshot.get("broker_imported")
        ),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }


def _walk_payload(value: Any, prefix: str = "") -> list[tuple[str, str, Any]]:
    rows: list[tuple[str, str, Any]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            rows.append((path, key_text, child))
            rows.extend(_walk_payload(child, path))
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for index, child in enumerate(value):
            rows.extend(_walk_payload(child, f"{prefix}[{index}]"))
    return rows


def _dry_snapshot_status(intake: Mapping[str, Any]) -> str:
    if intake.get("snapshot_status") == "FAIL" or intake.get("redaction_status") == "FAIL":
        return "FAIL"
    if intake.get("redaction_status") == "PASS_WITH_WARNINGS":
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _guardrail_status(guardrail_summary: Mapping[str, Any]) -> str:
    if _safe_float(guardrail_summary.get("blocked_count")) > 0:
        return "BLOCKED"
    if _safe_float(guardrail_summary.get("capped_count")) > 0:
        return "PASS_WITH_WARNINGS"
    return "PASS"


def _self_financing_paper_deltas(checks: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    non_cash: dict[str, float] = {}
    cash_symbol = "CASH"
    for row in checks:
        symbol = _text(row.get("symbol")).upper()
        if not symbol:
            continue
        delta = 0.0 if row.get("blocked") else _safe_float(row.get("capped_delta"))
        if symbol == cash_symbol:
            continue
        non_cash[symbol] = round(delta, 6)
    cash_delta = round(-sum(non_cash.values()), 6)
    return {cash_symbol: cash_delta, **dict(sorted(non_cash.items()))}


def _paper_action_source_paths(
    *,
    owner_review_id: str,
    dry_run_id: str,
    snapshot_id: str,
    drift_id: str,
    guardrail_id: str,
    owner_review_dir: Path,
    dry_run_dir: Path,
    manual_snapshot_dir: Path,
    drift_dir: Path,
    guardrail_dir: Path,
) -> dict[str, Path]:
    return {
        "owner_review_manifest": (
            owner_review_dir / owner_review_id / "real_execution_owner_review_manifest.json"
        ),
        "owner_execution_decision": (
            owner_review_dir / owner_review_id / "owner_execution_decision.json"
        ),
        "dry_run_manifest": dry_run_dir / dry_run_id / "real_snapshot_dry_run_manifest.json",
        "dry_run_artifact_links": dry_run_dir / dry_run_id / "dry_run_artifact_links.json",
        "normalized_portfolio": (
            manual_snapshot_dir / snapshot_id / "normalized_portfolio.json"
        ),
        "position_drift_manifest": drift_dir / drift_id / "position_drift_manifest.json",
        "consensus_drift_summary": drift_dir / drift_id / "consensus_drift_summary.json",
        "guardrail_manifest": guardrail_dir / guardrail_id / "guardrail_manifest.json",
        "proposed_adjustment_checks": (
            guardrail_dir / guardrail_id / "proposed_adjustment_checks.jsonl"
        ),
    }


def _apply_deltas(before: Mapping[str, float], deltas: Mapping[str, float]) -> dict[str, float]:
    symbols = sorted(set(before) | set(deltas))
    after = {
        symbol: max(
            0.0,
            round(_safe_float(before.get(symbol)) + _safe_float(deltas.get(symbol)), 6),
        )
        for symbol in symbols
    }
    total = sum(after.values())
    if total and abs(total - 1.0) > 0.000001:
        after = {symbol: round(value / total, 6) for symbol, value in after.items()}
    residual = round(1.0 - sum(after.values()), 6)
    if residual and after:
        cash_symbol = "CASH" if "CASH" in after else sorted(after)[0]
        after[cash_symbol] = round(after[cash_symbol] + residual, 6)
    return after


def _weekly_source_selection(
    *,
    week_ending: date,
    dry_run_dir: Path,
    owner_review_dir: Path,
    paper_action_dir: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[str]]:
    issues = [
        *_manifest_timestamp_issues(
            root=dry_run_dir,
            filename="real_snapshot_dry_run_manifest.json",
            label="dry_run",
        ),
        *_manifest_timestamp_issues(
            root=owner_review_dir,
            filename="real_execution_owner_review_manifest.json",
            label="owner_review",
        ),
        *_manifest_timestamp_issues(
            root=paper_action_dir,
            filename="real_snapshot_paper_action_manifest.json",
            label="paper_action",
        ),
    ]
    dry_candidates = _manifest_candidates_at_or_before(
        root=dry_run_dir,
        filename="real_snapshot_dry_run_manifest.json",
        week_ending=week_ending,
    )
    owner_candidates = _manifest_candidates_at_or_before(
        root=owner_review_dir,
        filename="real_execution_owner_review_manifest.json",
        week_ending=week_ending,
    )
    paper_candidates = _manifest_candidates_at_or_before(
        root=paper_action_dir,
        filename="real_snapshot_paper_action_manifest.json",
        week_ending=week_ending,
    )
    owner: dict[str, Any] = {}
    dry_run: dict[str, Any] = {}
    paper: dict[str, Any] = {}
    if owner_candidates:
        _, owner_path, owner_manifest = owner_candidates[-1]
        owner_decision = (
            _read_optional_json(owner_path.parent / "owner_execution_decision.json") or {}
        )
        if owner_manifest.get("owner_decision") != owner_decision.get("owner_decision"):
            issues.append("owner_manifest_decision_mismatch")
        owner = {**owner_manifest, **owner_decision, "status": owner_manifest.get("status")}
        dry_run_id = _text(owner.get("dry_run_id"))
        matching_dry = [
            row for row in dry_candidates if row[2].get("dry_run_id") == dry_run_id
        ]
        if not matching_dry:
            issues.append(f"owner_dry_run_missing:{dry_run_id or 'MISSING'}")
        else:
            _, dry_path, dry_manifest = matching_dry[-1]
            dry_summary = _read_optional_json(
                dry_path.parent / "real_snapshot_dry_run_summary.json"
            ) or {}
            dry_run = {
                **dry_manifest,
                **dry_summary,
                "dry_run_artifact_links": _read_optional_json(
                    dry_path.parent / "dry_run_artifact_links.json"
                )
                or {},
            }
        matching_paper = [
            row
            for row in paper_candidates
            if row[2].get("owner_review_id") == owner.get("review_id")
            and row[2].get("dry_run_id") == dry_run_id
        ]
        if matching_paper:
            paper = dict(matching_paper[-1][2])
    elif dry_candidates:
        _, dry_path, dry_manifest = dry_candidates[-1]
        dry_summary = _read_optional_json(
            dry_path.parent / "real_snapshot_dry_run_summary.json"
        ) or {}
        dry_run = {
            **dry_manifest,
            **dry_summary,
            "dry_run_artifact_links": _read_optional_json(
                dry_path.parent / "dry_run_artifact_links.json"
            )
            or {},
        }
    return dry_run, owner, paper, issues


def _weekly_source_validation(
    *,
    dry_run: Mapping[str, Any],
    owner: Mapping[str, Any],
    paper: Mapping[str, Any],
    dry_run_dir: Path,
    owner_review_dir: Path,
    paper_action_dir: Path,
    manual_snapshot_dir: Path,
    drift_dir: Path,
    guardrail_dir: Path,
) -> dict[str, dict[str, Any]]:
    validations: dict[str, dict[str, Any]] = {}
    if dry_run:
        validations["dry_run"] = validate_real_snapshot_dry_run(
            dry_run_id=_text(dry_run.get("dry_run_id")),
            output_dir=dry_run_dir,
        )
    if owner:
        validations["owner_review"] = validate_real_execution_owner_review(
            review_id=_text(owner.get("review_id")),
            output_dir=owner_review_dir,
        )
    if paper:
        validations["paper_action"] = validate_real_snapshot_paper_action(
            paper_action_id=_text(paper.get("paper_action_id")),
            output_dir=paper_action_dir,
            owner_review_dir=owner_review_dir,
            dry_run_dir=dry_run_dir,
            manual_snapshot_dir=manual_snapshot_dir,
            drift_dir=drift_dir,
            guardrail_dir=guardrail_dir,
        )
    return validations


def _weekly_source_paths(
    *,
    dry_run: Mapping[str, Any],
    owner: Mapping[str, Any],
    paper: Mapping[str, Any],
    dry_run_dir: Path,
    owner_review_dir: Path,
    paper_action_dir: Path,
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    if dry_run:
        dry_dir = dry_run_dir / _text(dry_run.get("dry_run_id"))
        paths.update(
            {
                "dry_run_manifest": dry_dir / "real_snapshot_dry_run_manifest.json",
                "dry_run_artifact_links": dry_dir / "dry_run_artifact_links.json",
                "dry_run_summary": dry_dir / "real_snapshot_dry_run_summary.json",
            }
        )
    if owner:
        owner_dir = owner_review_dir / _text(owner.get("review_id"))
        paths.update(
            {
                "owner_review_manifest": (
                    owner_dir / "real_execution_owner_review_manifest.json"
                ),
                "owner_execution_decision": owner_dir / "owner_execution_decision.json",
            }
        )
    if paper:
        paper_dir = paper_action_dir / _text(paper.get("paper_action_id"))
        paths.update(
            {
                "paper_action_manifest": paper_dir / "real_snapshot_paper_action_manifest.json",
                "paper_action": paper_dir / "paper_action_from_real_snapshot.json",
                "paper_state": paper_dir / "paper_state_after_action.json",
            }
        )
    return paths


def _owner_decision_inventory_at_or_before(
    *,
    owner_review_dir: Path,
    week_ending: date,
) -> tuple[dict[str, Any], list[str]]:
    reviews: list[dict[str, Any]] = []
    issues: list[str] = []
    for decision_path in sorted(owner_review_dir.glob("*/owner_execution_decision.json")):
        decision = _read_optional_json(decision_path) or {}
        observed_at = _artifact_datetime(decision)
        review_id = _text(decision.get("review_id"), decision_path.parent.name)
        if observed_at is None:
            issues.append(f"owner_decision_timestamp_invalid:{review_id}")
            continue
        if observed_at.date() > week_ending:
            continue
        manifest_path = decision_path.parent / "real_execution_owner_review_manifest.json"
        manifest = _read_optional_json(manifest_path) or {}
        validation = validate_real_execution_owner_review(
            review_id=review_id,
            output_dir=owner_review_dir,
        )
        if validation.get("status") != "PASS":
            issues.append(f"owner_review_validation_failed:{review_id}")
            continue
        if manifest.get("updated_at") != decision.get("updated_at"):
            issues.append(f"owner_review_timestamp_mismatch:{review_id}")
            continue
        reviews.append(
            {
                "review_id": review_id,
                "dry_run_id": _text(decision.get("dry_run_id")),
                "owner_decision": _text(decision.get("owner_decision"), "pending"),
                "updated_at": observed_at.isoformat(),
                "manifest_path": str(manifest_path),
                "manifest_sha256": _file_sha256(manifest_path),
                "decision_path": str(decision_path),
                "decision_sha256": _file_sha256(decision_path),
            }
        )
    reviews.sort(key=lambda row: (row["updated_at"], row["review_id"]))
    return (
        {
            "schema_version": SCHEMA_VERSION,
            "report_type": "etf_dynamic_v3_weekly_owner_decision_inventory",
            "week_ending": week_ending.isoformat(),
            "review_count": len(reviews),
            "reviews": reviews,
        },
        issues,
    )


def _manifest_candidates_at_or_before(
    *,
    root: Path,
    filename: str,
    week_ending: date,
) -> list[tuple[datetime, Path, dict[str, Any]]]:
    rows: list[tuple[datetime, Path, dict[str, Any]]] = []
    for path in root.glob(f"*/{filename}"):
        payload = _read_optional_json(path) or {}
        observed_at = _artifact_datetime(payload)
        if observed_at is not None and observed_at.date() <= week_ending:
            rows.append((observed_at, path, payload))
    rows.sort(key=lambda row: (row[0], row[1].as_posix()))
    return rows


def _manifest_timestamp_issues(*, root: Path, filename: str, label: str) -> list[str]:
    return [
        f"{label}_timestamp_invalid:{path.parent.name}"
        for path in root.glob(f"*/{filename}")
        if _artifact_datetime(_read_optional_json(path) or {}) is None
    ]


def _artifact_datetime(payload: Mapping[str, Any]) -> datetime | None:
    for key in ("updated_at", "generated_at", "created_at"):
        value = _text(payload.get(key))
        if not value:
            continue
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.replace(tzinfo=parsed.tzinfo or UTC).astimezone(UTC)
    return None


def _weekly_summary_payload(
    *,
    weekly_real_review_id: str,
    week_ending: date,
    dry_run: Mapping[str, Any],
    owner: Mapping[str, Any],
    paper: Mapping[str, Any],
) -> dict[str, Any]:
    links = _mapping(dry_run.get("dry_run_artifact_links")) if dry_run else {}
    owner_decision = _text(owner.get("owner_decision"), "pending") if owner else "pending"
    paper_action_taken = _text(paper.get("action_type")) == "paper_only" if paper else False
    chain_status = _weekly_chain_status(dry_run, owner, paper, owner_decision)
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": "etf_dynamic_v3_weekly_real_snapshot_summary",
        "weekly_real_review_id": weekly_real_review_id,
        "week_ending": week_ending.isoformat(),
        "chain_status": chain_status,
        "latest_snapshot_id": _text(links.get("manual_portfolio_snapshot_id"), "MISSING"),
        "latest_dry_run_id": _text(dry_run.get("dry_run_id"), "MISSING"),
        "latest_owner_review_id": _text(owner.get("review_id"), "MISSING"),
        "latest_paper_action_id": _text(paper.get("paper_action_id"), "MISSING"),
        "snapshot_status": _text(dry_run.get("snapshot_status"), "MISSING"),
        "recommended_action": _text(
            dry_run.get("manual_review_recommended_action"),
            "blocked",
        ),
        "owner_decision": owner_decision,
        "paper_action_taken": paper_action_taken,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "next_action": _weekly_next_action(
            dry_run,
            owner_decision,
            paper_action_taken,
            bool(paper),
        ),
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }


def _weekly_chain_status(
    dry_run: Mapping[str, Any],
    owner: Mapping[str, Any],
    paper: Mapping[str, Any],
    owner_decision: str,
) -> str:
    if not dry_run:
        return "MISSING_DRY_RUN"
    if not owner:
        return "OWNER_REVIEW_MISSING"
    if owner_decision == "pending":
        return "OWNER_DECISION_PENDING"
    if not paper:
        return "PAPER_ACTION_MISSING"
    return "COMPLETE"


def _weekly_next_action(
    dry_run: Mapping[str, Any],
    owner_decision: str,
    paper_action_taken: bool,
    paper_action_present: bool,
) -> str:
    if not dry_run:
        return "update_snapshot"
    if owner_decision == "pending":
        return "owner_review_required"
    if not paper_action_present:
        return "paper_action_tracking_required"
    if owner_decision == "paper_adjustment_review_only" and paper_action_taken:
        return "paper_track"
    if owner_decision in {"needs_more_data", "defer"}:
        return "needs_more_data"
    return "continue_monitoring"


def _owner_decision_summary_from_inventory(inventory: Mapping[str, Any]) -> dict[str, int]:
    counts = {
        "pending_reviews": 0,
        "monitor_count": 0,
        "no_trade_count": 0,
        "paper_adjustment_review_only_count": 0,
        "reject_advisory_count": 0,
        "needs_more_data_count": 0,
        "defer_count": 0,
    }
    reviews = inventory.get("reviews")
    for row in reviews if isinstance(reviews, list) else []:
        decision = _text(_mapping(row).get("owner_decision"), "pending")
        if decision == "pending":
            counts["pending_reviews"] += 1
        elif f"{decision}_count" in counts:
            counts[f"{decision}_count"] += 1
    return counts


def _validation_payload(
    report_type: str,
    checks: Sequence[Mapping[str, Any]],
    artifact_id: str,
) -> dict[str, Any]:
    status = "PASS" if all(check.get("passed") is True for check in checks) else "FAIL"
    return {
        "schema_version": SCHEMA_VERSION,
        "report_type": report_type,
        "artifact_id": artifact_id,
        "status": status,
        "checks": list(checks),
        "failed_check_count": sum(1 for check in checks if check.get("passed") is not True),
        "broker_action_allowed": False,
        "broker_action_taken": False,
        "order_ticket_generated": False,
        "production_effect": PRODUCTION_EFFECT,
        **_safety(),
    }


def _required_file_checks(root: Path, names: Sequence[str]) -> list[dict[str, Any]]:
    return [
        _check(f"artifact_exists:{name}", (root / name).exists(), str(root / name))
        for name in names
    ]


def _check(check_id: str, passed: bool, detail: str) -> dict[str, Any]:
    return {"check_id": check_id, "passed": bool(passed), "detail": detail}


def _payload_safe(*payloads: Mapping[str, Any]) -> bool:
    return all(
        payload.get("broker_action_allowed") is not True
        and payload.get("broker_action_taken") is not True
        and payload.get("order_ticket_generated") is not True
        and payload.get("production_state_mutated") is not True
        and payload.get("baseline_config_mutated") is not True
        and payload.get("official_target_weights_mutated") is not True
        and payload.get("automatic_candidate_promotion") is not True
        and payload.get("auto_enrollment_without_owner_approval") is not True
        and payload.get("owner_approval_executed") is not True
        and _text(payload.get("production_effect"), PRODUCTION_EFFECT) == PRODUCTION_EFFECT
        for payload in payloads
        if payload
    )


def _safety() -> dict[str, Any]:
    return dict(DYNAMIC_V3_PARAMETER_RESEARCH_SAFETY)


def _stable_id(*parts: object) -> str:
    digest = sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()
    return digest[:16]


def _file_sha256(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _unique_dir(path: Path) -> Path:
    if not path.exists():
        return path
    for index in range(1, 1000):
        candidate = path.with_name(f"{path.name}-{index:03d}")
        if not candidate.exists():
            return candidate
    raise RealSnapshotError(f"unable to allocate unique artifact dir under {path.parent}")


def _write_latest_pointer(pointer_name: str, artifact_id: str, path: Path) -> None:
    if not _is_default_dynamic_v3_research_artifact(path):
        return
    DEFAULT_LATEST_POINTER_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_type": pointer_name.removeprefix("latest_"),
        "artifact_id": artifact_id,
        "path": str(path),
        "exists": path.exists(),
        "updated_at": datetime.now(UTC).isoformat(),
    }
    _write_json(DEFAULT_LATEST_POINTER_DIR / f"{pointer_name}.json", payload)


def _is_default_dynamic_v3_research_artifact(path: Path) -> bool:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = DEFAULT_DYNAMIC_V3_RESEARCH_ROOT.resolve(strict=False)
        return resolved_path == resolved_root or resolved_path.is_relative_to(resolved_root)
    except (OSError, RuntimeError, ValueError):
        return False


def _latest_pointer_artifact_id(pointer_name: str) -> str:
    pointer = _read_optional_json(DEFAULT_LATEST_POINTER_DIR / f"{pointer_name}.json") or {}
    return _text(pointer.get("artifact_id"))


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise RealSnapshotError(f"JSON payload must be an object: {path}")
    return payload


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return _read_json(path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                payload = json.loads(line)
                if isinstance(payload, dict):
                    rows.append(payload)
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _float_mapping(value: Any) -> dict[str, float]:
    mapping = _mapping(value)
    return {str(key): round(_safe_float(raw), 6) for key, raw in mapping.items()}


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value)
    return text if text else default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
