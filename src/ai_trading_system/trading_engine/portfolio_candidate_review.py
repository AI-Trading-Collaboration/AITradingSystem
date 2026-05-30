from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml

from ai_trading_system.config import PROJECT_ROOT
from ai_trading_system.shadow.lineage import git_commit_sha, git_worktree_dirty, sha256_file
from ai_trading_system.trading_engine.parameters.parameter_loader import resolve_project_path
from ai_trading_system.trading_engine.portfolio_candidates import (
    latest_portfolio_candidates_path,
    latest_portfolio_candidates_path_on_or_before,
)
from ai_trading_system.yaml_loader import safe_load_yaml_path

PORTFOLIO_CANDIDATE_REVIEW_SCHEMA_VERSION = 1
PORTFOLIO_CANDIDATE_REVIEW_PACKAGE_REPORT_TYPE = "portfolio_candidate_review_package"
PORTFOLIO_CANDIDATE_REVIEW_DECISION_REPORT_TYPE = "portfolio_candidate_review_decision"
PORTFOLIO_CANDIDATE_REVIEW_ALIAS_REPORT_TYPE = "portfolio_candidate_review_report"
DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH = (
    PROJECT_ROOT / "config" / "portfolio" / "portfolio_candidate_review.yaml"
)
REVIEW_STATUSES = {
    "pending_review",
    "approved_for_shadow_candidate",
    "rejected",
    "watch",
    "needs_more_data",
}


@dataclass(frozen=True)
class PortfolioCandidateReviewRun:
    as_of: date
    package_payload: dict[str, Any]
    decision_payload: dict[str, Any]
    package_json_path: Path
    package_markdown_path: Path
    decision_json_path: Path
    decision_markdown_path: Path


@dataclass(frozen=True)
class PortfolioCandidateDecisionRun:
    as_of: date
    package_payload: dict[str, Any]
    decision_payload: dict[str, Any]
    decision_json_path: Path
    decision_markdown_path: Path


def default_portfolio_candidate_reviews_root() -> Path:
    return PROJECT_ROOT / "artifacts" / "portfolio_candidate_reviews"


def default_portfolio_candidate_review_dir(output_root: Path, as_of: date) -> Path:
    return output_root / as_of.isoformat()


def default_portfolio_candidate_review_package_json_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_candidate_review_dir(output_root, as_of)
        / "portfolio_candidate_review_package.json"
    )


def default_portfolio_candidate_review_package_markdown_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_candidate_review_dir(output_root, as_of)
        / "portfolio_candidate_review_package.md"
    )


def default_portfolio_candidate_review_decision_json_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_candidate_review_dir(output_root, as_of)
        / "portfolio_candidate_review_decision.json"
    )


def default_portfolio_candidate_review_decision_markdown_path(
    output_root: Path,
    as_of: date,
) -> Path:
    return (
        default_portfolio_candidate_review_dir(output_root, as_of)
        / "portfolio_candidate_review_decision.md"
    )


def latest_portfolio_candidate_review_decision_path(
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_candidate_reviews_root()
    candidates = sorted(root.glob("*/portfolio_candidate_review_decision.json"))
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def latest_portfolio_candidate_review_decision_path_on_or_before(
    as_of: date,
    output_root: Path | None = None,
) -> Path | None:
    root = output_root or default_portfolio_candidate_reviews_root()
    candidates: list[tuple[date, Path]] = []
    for path in root.glob("*/portfolio_candidate_review_decision.json"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[1].stat().st_mtime, item[0]))[1]


def portfolio_candidate_review_report_alias_paths(
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    return (
        reports_dir / f"portfolio_candidate_review_{as_of.isoformat()}.json",
        reports_dir / f"portfolio_candidate_review_{as_of.isoformat()}.md",
    )


def load_portfolio_candidate_review_config(
    path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
) -> dict[str, Any]:
    payload = safe_load_yaml_path(Path(path))
    if not isinstance(payload, dict):
        raise ValueError(f"portfolio candidate review config must be a mapping: {path}")
    _validate_review_config(payload)
    return payload


def run_portfolio_candidate_review(
    *,
    as_of: date | None = None,
    candidate_path: Path | None = None,
    reviewer: str | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> PortfolioCandidateReviewRun:
    config = load_portfolio_candidate_review_config(config_path)
    generated = generated_at or datetime.now(tz=UTC)
    package_payload = build_portfolio_candidate_review_package(
        as_of=as_of,
        candidate_path=candidate_path,
        config=config,
        config_path=Path(config_path),
        generated_at=generated,
    )
    resolved_as_of = portfolio_candidate_review_payload_date(
        package_payload,
        Path(str(package_payload.get("candidate", {}).get("source_artifact") or "")),
    )
    output_root = _output_root(config)
    package_json_path = default_portfolio_candidate_review_package_json_path(
        output_root,
        resolved_as_of,
    )
    package_markdown_path = default_portfolio_candidate_review_package_markdown_path(
        output_root,
        resolved_as_of,
    )
    decision_json_path = default_portfolio_candidate_review_decision_json_path(
        output_root,
        resolved_as_of,
    )
    decision_markdown_path = default_portfolio_candidate_review_decision_markdown_path(
        output_root,
        resolved_as_of,
    )
    write_portfolio_candidate_review_package(
        package_payload,
        package_json_path,
        package_markdown_path,
    )
    if decision_json_path.exists():
        decision_payload = load_portfolio_candidate_review_payload(decision_json_path)
    else:
        decision_payload = build_portfolio_candidate_review_decision(
            package_payload=package_payload,
            decision="pending_review",
            reason=None,
            reviewer=reviewer,
            config=config,
            generated_at=generated,
            package_path=package_json_path,
        )
        write_portfolio_candidate_review_decision(
            decision_payload,
            decision_json_path,
            decision_markdown_path,
        )
    return PortfolioCandidateReviewRun(
        as_of=resolved_as_of,
        package_payload=package_payload,
        decision_payload=decision_payload,
        package_json_path=package_json_path,
        package_markdown_path=package_markdown_path,
        decision_json_path=decision_json_path,
        decision_markdown_path=decision_markdown_path,
    )


def decide_portfolio_candidate(
    *,
    decision: str,
    as_of: date | None = None,
    candidate_path: Path | None = None,
    reviewer: str | None = None,
    reason: str | None = None,
    config_path: Path | str = DEFAULT_PORTFOLIO_CANDIDATE_REVIEW_CONFIG_PATH,
    generated_at: datetime | None = None,
) -> PortfolioCandidateDecisionRun:
    if decision not in REVIEW_STATUSES - {"pending_review"}:
        raise ValueError(f"unsupported portfolio candidate review decision: {decision}")
    config = load_portfolio_candidate_review_config(config_path)
    generated = generated_at or datetime.now(tz=UTC)
    output_root = _output_root(config)
    package_path = _resolve_review_package_path(
        output_root=output_root,
        as_of=as_of,
        candidate_path=candidate_path,
    )
    if package_path is None or not package_path.exists():
        review_run = run_portfolio_candidate_review(
            as_of=as_of,
            candidate_path=candidate_path,
            reviewer=reviewer,
            config_path=config_path,
            generated_at=generated,
        )
        package_payload = review_run.package_payload
        package_path = review_run.package_json_path
        resolved_as_of = review_run.as_of
    else:
        package_payload = load_portfolio_candidate_review_payload(package_path)
        resolved_as_of = portfolio_candidate_review_payload_date(package_payload, package_path)
    decision_payload = build_portfolio_candidate_review_decision(
        package_payload=package_payload,
        decision=decision,
        reason=reason,
        reviewer=reviewer,
        config=config,
        generated_at=generated,
        package_path=package_path,
    )
    decision_json_path = default_portfolio_candidate_review_decision_json_path(
        output_root,
        resolved_as_of,
    )
    decision_markdown_path = default_portfolio_candidate_review_decision_markdown_path(
        output_root,
        resolved_as_of,
    )
    write_portfolio_candidate_review_decision(
        decision_payload,
        decision_json_path,
        decision_markdown_path,
    )
    return PortfolioCandidateDecisionRun(
        as_of=resolved_as_of,
        package_payload=package_payload,
        decision_payload=decision_payload,
        decision_json_path=decision_json_path,
        decision_markdown_path=decision_markdown_path,
    )


def build_portfolio_candidate_review_package(
    *,
    as_of: date | None,
    candidate_path: Path | None,
    config: dict[str, Any],
    config_path: Path,
    generated_at: datetime,
) -> dict[str, Any]:
    resolved_config_path = resolve_project_path(str(config_path))
    candidate_artifact = _resolve_candidate_artifact(config, as_of, candidate_path)
    resolved_as_of = candidate_artifact.as_of or as_of or generated_at.date()
    candidate_payload = _load_yaml_mapping(candidate_artifact.recommended_path)
    candidates_payload = _load_json_mapping(candidate_artifact.summary_path)
    supporting = _supporting_artifacts(
        config=config,
        as_of=resolved_as_of,
        candidate_path=candidate_artifact.recommended_path,
        candidates_path=candidate_artifact.summary_path,
    )
    production_path = _production_parameters_path(config)
    production_sha = _sha256_if_exists(production_path)
    production_modified = _git_path_modified(production_path)
    candidate_sha = _sha256_if_exists(candidate_artifact.recommended_path)
    evidence = _evidence_summary(
        candidates_payload=candidates_payload,
        supporting=supporting,
    )
    profile_name = _candidate_profile_name(candidate_payload, candidates_payload)
    candidate_version = _candidate_version(resolved_as_of, profile_name, candidate_sha)
    hard_rejections = _hard_rejections(
        package_like={
            "candidate": {
                "source_artifact": _path_text(candidate_artifact.recommended_path),
            },
            "current_production": {
                "sha256": production_sha,
                "modified": production_modified,
            },
            "evidence_summary": evidence,
            "metadata": {
                "production_effect": "none",
                "auto_promotion": False,
            },
            "missing_supporting_artifacts": supporting["missing_supporting_artifacts"],
        },
        config=config,
    )
    return {
        "schema_version": PORTFOLIO_CANDIDATE_REVIEW_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATE_REVIEW_PACKAGE_REPORT_TYPE,
        "metadata": {
            "run_id": f"portfolio-candidate-review-{resolved_as_of.isoformat()}",
            "generated_at": generated_at.isoformat(),
            "status": "PENDING_REVIEW",
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "observe_only": True,
            "config_path": str(resolved_config_path),
            "git_commit": git_commit_sha(),
            "git_worktree_dirty": git_worktree_dirty(),
        },
        "candidate": {
            "profile_name": profile_name,
            "candidate_version": candidate_version,
            "source_artifact": _path_text(candidate_artifact.recommended_path),
            "candidate_hash": candidate_sha,
            "recommended_by": _recommended_by(candidate_artifact.summary_path),
            "source_portfolio_candidates": candidate_payload.get(
                "source_portfolio_candidates",
                "",
            ),
        },
        "current_production": {
            "path": str(production_path),
            "sha256": production_sha,
            "modified": production_modified,
        },
        "evidence_summary": evidence,
        "risk_summary": {
            "signal_quality_limited": evidence.get("signal_snapshot_status") == "LIMITED",
            "fallback_signals_present": _fallback_signals_present(supporting),
            "candidate_promotion_disabled": True,
            "production_write_allowed": False,
            "candidate_production_promotion_allowed": False,
        },
        "manual_review_checklist": _manual_review_checklist(
            evidence=evidence,
            production_modified=production_modified,
            hard_rejections=hard_rejections,
        ),
        "supporting_artifacts": supporting["supporting_artifacts"],
        "missing_supporting_artifacts": supporting["missing_supporting_artifacts"],
        "policy": {
            "version": config.get("version", "UNKNOWN"),
            "review_status": list(_review_statuses(config)),
            "decision_rules": _mapping(config.get("decision_rules")),
            "allowed_next_steps": _mapping(config.get("allowed_next_steps")),
        },
        "candidate_summary": {
            "status": evidence.get("candidate_summary_status", "UNKNOWN"),
            "best_profile": evidence.get("best_profile", ""),
            "ranking_reason": _mapping(candidates_payload.get("ranking")).get("reason", ""),
        },
        "rollback_reference": {
            "production_path": str(production_path),
            "production_sha256_before_review": production_sha,
            "candidate_artifact": _path_text(candidate_artifact.recommended_path),
            "rollback_required": False,
            "reason": "Review workflow does not write production parameters.",
        },
        "allowed_decisions": [
            "approved_for_shadow_candidate",
            "rejected",
            "watch",
            "needs_more_data",
        ],
        "recommended_next_step": _recommended_next_step(evidence, hard_rejections, config),
        "hard_rejections": hard_rejections,
        "safety": {
            "production_config_modified": production_modified,
            "production_write_allowed": False,
            "candidate_promotion_enabled": False,
            "candidate_production_promotion_allowed": False,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
    }


def build_portfolio_candidate_review_decision(
    *,
    package_payload: dict[str, Any],
    decision: str,
    reason: str | None,
    reviewer: str | None,
    config: dict[str, Any],
    generated_at: datetime,
    package_path: Path,
) -> dict[str, Any]:
    requested_decision = decision
    package_date = portfolio_candidate_review_payload_date(package_payload, package_path)
    candidate = _mapping(package_payload.get("candidate"))
    current_production = _mapping(package_payload.get("current_production"))
    current_sha = _sha256_if_exists(Path(str(current_production.get("path") or "")))
    package_sha = str(current_production.get("sha256") or "")
    production_modified = bool(current_production.get("modified")) or (
        bool(package_sha) and bool(current_sha) and current_sha != package_sha
    )
    package_for_decision = {
        **package_payload,
        "current_production": {
            **current_production,
            "modified": production_modified,
            "current_sha256": current_sha,
        },
    }
    hard_rejections = _hard_rejections(package_like=package_for_decision, config=config)
    final_decision = decision
    if decision == "approved_for_shadow_candidate" and hard_rejections:
        final_decision = "rejected"
    default_decision = _mapping(config.get("default_decision"))
    reviewer_name = reviewer or str(default_decision.get("reviewer") or "manual")
    decision_reason = reason or _default_reason(final_decision, hard_rejections, config)
    if final_decision != requested_decision:
        decision_reason = (
            "Requested approved_for_shadow_candidate was rejected by hard rejection "
            f"rules: {', '.join(hard_rejections)}. {decision_reason}"
        )
    allowed_next_step = str(
        _mapping(config.get("allowed_next_steps")).get(
            final_decision,
            "continue_shadow_tracking" if final_decision == "watch" else "manual_review",
        )
    )
    return {
        "schema_version": PORTFOLIO_CANDIDATE_REVIEW_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATE_REVIEW_DECISION_REPORT_TYPE,
        "metadata": {
            "decision_id": f"portfolio-candidate-decision-{package_date.isoformat()}",
            "created_at": generated_at.isoformat(),
            "status": final_decision,
            "requested_status": requested_decision,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
            "source_review_package": str(package_path),
        },
        "decision": {
            "status": final_decision,
            "requested_status": requested_decision,
            "reviewer": reviewer_name,
            "reason": decision_reason,
            "allowed_next_step": allowed_next_step,
            "production_write_allowed": False,
        },
        "candidate": {
            "profile_name": candidate.get("profile_name", ""),
            "candidate_version": candidate.get("candidate_version", ""),
            "candidate_hash": candidate.get("candidate_hash", ""),
            "source_artifact": candidate.get("source_artifact", ""),
        },
        "evidence_summary": _mapping(package_payload.get("evidence_summary")),
        "hard_rejections": hard_rejections,
        "rollback_reference": _mapping(package_payload.get("rollback_reference")),
        "supporting_artifacts": {
            "portfolio_candidate_review_package": str(package_path),
            **_mapping(package_payload.get("supporting_artifacts")),
        },
        "safety": {
            "production_config_modified": production_modified,
            "candidate_promotion_enabled": False,
            "candidate_production_promotion_allowed": False,
            "requires_future_review": True,
            "production_write_allowed": False,
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
    }


def write_portfolio_candidate_review_package(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_portfolio_candidate_review_package_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def write_portfolio_candidate_review_decision(
    payload: dict[str, Any],
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(
        render_portfolio_candidate_review_decision_markdown(payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def write_portfolio_candidate_review_report_alias(
    package_payload: dict[str, Any],
    decision_payload: dict[str, Any],
    reports_dir: Path,
    as_of: date,
) -> tuple[Path, Path]:
    alias_payload = {
        "schema_version": PORTFOLIO_CANDIDATE_REVIEW_SCHEMA_VERSION,
        "report_type": PORTFOLIO_CANDIDATE_REVIEW_ALIAS_REPORT_TYPE,
        "source_report_types": [
            PORTFOLIO_CANDIDATE_REVIEW_PACKAGE_REPORT_TYPE,
            PORTFOLIO_CANDIDATE_REVIEW_DECISION_REPORT_TYPE,
        ],
        "metadata": {
            "run_id": f"portfolio-candidate-review-report-{as_of.isoformat()}",
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "status": _mapping(decision_payload.get("decision")).get("status", "UNKNOWN"),
            "production_effect": "none",
            "manual_review_required": True,
            "auto_promotion": False,
        },
        "package": package_payload,
        "decision": decision_payload,
    }
    json_path, markdown_path = portfolio_candidate_review_report_alias_paths(
        reports_dir,
        as_of,
    )
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(alias_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    markdown_path.write_text(
        render_portfolio_candidate_review_alias_markdown(alias_payload),
        encoding="utf-8",
    )
    return json_path, markdown_path


def load_portfolio_candidate_review_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def validate_portfolio_candidate_review_decision_payload(
    payload: dict[str, Any],
) -> list[str]:
    issues: list[str] = []
    if payload.get("schema_version") != PORTFOLIO_CANDIDATE_REVIEW_SCHEMA_VERSION:
        issues.append("schema_version mismatch")
    if payload.get("report_type") != PORTFOLIO_CANDIDATE_REVIEW_DECISION_REPORT_TYPE:
        issues.append("report_type mismatch")
    metadata = _mapping(payload.get("metadata"))
    decision = _mapping(payload.get("decision"))
    safety = _mapping(payload.get("safety"))
    if metadata.get("production_effect") != "none":
        issues.append("production_effect must be none")
    if metadata.get("manual_review_required") is not True:
        issues.append("manual_review_required must be true")
    if metadata.get("auto_promotion") is not False:
        issues.append("auto_promotion must be false")
    status = str(decision.get("status") or metadata.get("status") or "")
    if status not in REVIEW_STATUSES:
        issues.append("decision status is invalid")
    if decision.get("production_write_allowed") is not False:
        issues.append("production_write_allowed must be false")
    if safety.get("production_config_modified") is not False:
        issues.append("production_config_modified must be false")
    if safety.get("candidate_promotion_enabled") is not False:
        issues.append("candidate_promotion_enabled must be false")
    if safety.get("candidate_production_promotion_allowed") is not False:
        issues.append("candidate_production_promotion_allowed must be false")
    if safety.get("production_effect") != "none":
        issues.append("safety production_effect must be none")
    if safety.get("manual_review_required") is not True:
        issues.append("safety manual_review_required must be true")
    if safety.get("auto_promotion") is not False:
        issues.append("safety auto_promotion must be false")
    if status == "approved_for_shadow_candidate" and payload.get("hard_rejections"):
        issues.append("approved_for_shadow_candidate has hard rejections")
    return issues


def portfolio_candidate_review_payload_date(
    payload: dict[str, Any],
    source_path: Path,
) -> date:
    metadata = _mapping(payload.get("metadata"))
    for key, prefix in (
        ("run_id", "portfolio-candidate-review-"),
        ("decision_id", "portfolio-candidate-decision-"),
    ):
        raw = str(metadata.get(key) or "")
        if raw.startswith(prefix):
            try:
                return date.fromisoformat(raw.removeprefix(prefix))
            except ValueError:
                pass
    try:
        return date.fromisoformat(source_path.parent.name)
    except ValueError as exc:
        message = f"cannot infer portfolio candidate review date from {source_path}"
        raise ValueError(message) from exc


def render_portfolio_candidate_review_package_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    candidate = _mapping(payload.get("candidate"))
    evidence = _mapping(payload.get("evidence_summary"))
    risk = _mapping(payload.get("risk_summary"))
    production = _mapping(payload.get("current_production"))
    lines = [
        "# Portfolio Candidate Review Package",
        "",
        "## 1. Executive Summary",
        "",
        f"- review status: `{metadata.get('status', 'UNKNOWN')}`",
        f"- candidate profile: `{candidate.get('profile_name', '')}`",
        f"- recommended next step: `{payload.get('recommended_next_step', '')}`",
        "- 该 review package 只用于人工审阅，不会写入 production 参数。",
        "",
        "## 2. Candidate Profile",
        "",
        f"- profile_name: `{candidate.get('profile_name', '')}`",
        f"- candidate_version: `{candidate.get('candidate_version', '')}`",
        f"- source_artifact: `{candidate.get('source_artifact', '')}`",
        f"- candidate_hash: `{candidate.get('candidate_hash', '')}`",
        "",
        "## 3. Evidence Summary",
        "",
    ]
    for key, value in evidence.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 4. Current Production Hash",
            "",
            f"- path: `{production.get('path', '')}`",
            f"- sha256: `{production.get('sha256', '')}`",
            f"- modified: `{production.get('modified', False)}`",
            "",
            "## 5. Data Gate Status",
            "",
            f"- data_gate: `{evidence.get('data_gate', 'UNKNOWN')}`",
            "",
            "## 6. Signal Quality Status",
            "",
            f"- signal_snapshot_status: `{evidence.get('signal_snapshot_status', 'UNKNOWN')}`",
            f"- signal_quality_limited: `{risk.get('signal_quality_limited', False)}`",
            "",
            "## 7. Portfolio Candidate Results",
            "",
            f"- best_profile: `{evidence.get('best_profile', '')}`",
            "- can_support_candidate_promotion: "
            f"`{evidence.get('can_support_candidate_promotion', False)}`",
            "",
            "## 8. Risk Summary",
            "",
        ]
    )
    for key, value in risk.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 9. Manual Review Checklist",
            "",
            "| Item | Status | Reason |",
            "|---|---|---|",
        ]
    )
    for item in _records(payload.get("manual_review_checklist")):
        lines.append(
            f"| {item.get('item', '')} | `{item.get('status', '')}` | "
            f"{item.get('reason', '')} |"
        )
    lines.extend(
        [
            "",
            "## 10. Supporting Artifacts",
            "",
        ]
    )
    for key, value in _mapping(payload.get("supporting_artifacts")).items():
        lines.append(f"- `{key}`: `{value}`")
    missing = _records(payload.get("missing_supporting_artifacts"))
    if missing:
        lines.append("")
        lines.append("missing_supporting_artifacts:")
        for item in missing:
            lines.append(f"- `{item}`")
    lines.extend(
        [
            "",
            "## 11. Allowed Decisions",
            "",
        ]
    )
    for item in _records(payload.get("allowed_decisions")):
        lines.append(f"- `{item}`")
    lines.extend(
        [
            "",
            "## 12. Recommended Next Step",
            "",
            f"- `{payload.get('recommended_next_step', '')}`",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_portfolio_candidate_review_decision_markdown(payload: dict[str, Any]) -> str:
    metadata = _mapping(payload.get("metadata"))
    decision = _mapping(payload.get("decision"))
    candidate = _mapping(payload.get("candidate"))
    safety = _mapping(payload.get("safety"))
    lines = [
        "# Portfolio Candidate Review Decision",
        "",
        "## 1. Decision",
        "",
        f"- status: `{decision.get('status', metadata.get('status', 'UNKNOWN'))}`",
        f"- requested_status: `{decision.get('requested_status', '')}`",
        f"- allowed_next_step: `{decision.get('allowed_next_step', '')}`",
        "",
        "## 2. Reviewer",
        "",
        f"- reviewer: `{decision.get('reviewer', '')}`",
        f"- created_at: `{metadata.get('created_at', '')}`",
        "",
        "## 3. Reason",
        "",
        decision.get("reason", ""),
        "",
        "## 4. Candidate Profile",
        "",
        f"- profile_name: `{candidate.get('profile_name', '')}`",
        f"- candidate_version: `{candidate.get('candidate_version', '')}`",
        f"- candidate_hash: `{candidate.get('candidate_hash', '')}`",
        "",
        "## 5. Safety Confirmation",
        "",
    ]
    for key, value in safety.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.extend(
        [
            "",
            "## 6. Production Effect",
            "",
            f"- production_effect: `{metadata.get('production_effect', 'none')}`",
            "- production parameters remain unchanged by this workflow.",
            "",
            "## 7. Next Step",
            "",
            f"- `{decision.get('allowed_next_step', '')}`",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_portfolio_candidate_review_alias_markdown(payload: dict[str, Any]) -> str:
    decision_payload = _mapping(payload.get("decision"))
    package_payload = _mapping(payload.get("package"))
    decision = _mapping(decision_payload.get("decision"))
    candidate = _mapping(decision_payload.get("candidate"))
    evidence = _mapping(package_payload.get("evidence_summary"))
    decision_metadata = _mapping(decision_payload.get("metadata"))
    return "\n".join(
        [
            "# Portfolio Candidate Review Report",
            "",
            "## Summary",
            "",
            f"- decision: `{decision.get('status', '')}`",
            f"- candidate: `{candidate.get('profile_name', '')}`",
            f"- reviewer: `{decision.get('reviewer', '')}`",
            f"- signal_quality: `{evidence.get('signal_snapshot_status', 'UNKNOWN')}`",
            f"- production_effect: `{decision_metadata.get('production_effect', 'none')}`",
            "",
            "## Decision Reason",
            "",
            str(decision.get("reason") or ""),
        ]
    ).rstrip() + "\n"


@dataclass(frozen=True)
class _CandidateArtifact:
    as_of: date | None
    summary_path: Path | None
    recommended_path: Path | None


def _resolve_candidate_artifact(
    config: dict[str, Any],
    as_of: date | None,
    candidate_path: Path | None,
) -> _CandidateArtifact:
    if candidate_path is not None:
        path = resolve_project_path(str(candidate_path))
        candidate_date = _parent_date(path)
        summary_path = path.parent / "portfolio_candidates_summary.json"
        return _CandidateArtifact(candidate_date or as_of, summary_path, path)
    candidates_root = _input_root(config, "portfolio_candidates_dir")
    summary_path: Path | None
    if as_of is None:
        summary_path = latest_portfolio_candidates_path(candidates_root)
    else:
        exact = candidates_root / as_of.isoformat() / "portfolio_candidates_summary.json"
        summary_path = exact if exact.exists() else latest_portfolio_candidates_path_on_or_before(
            as_of,
            candidates_root,
        )
    if summary_path is None:
        return _CandidateArtifact(as_of, None, None)
    return _CandidateArtifact(
        _parent_date(summary_path) or as_of,
        summary_path,
        summary_path.parent / "recommended_portfolio_candidate.yaml",
    )


def _resolve_review_package_path(
    *,
    output_root: Path,
    as_of: date | None,
    candidate_path: Path | None,
) -> Path | None:
    if as_of is not None:
        return default_portfolio_candidate_review_package_json_path(output_root, as_of)
    if candidate_path is not None:
        candidate_date = _parent_date(resolve_project_path(str(candidate_path)))
        if candidate_date is not None:
            return default_portfolio_candidate_review_package_json_path(
                output_root,
                candidate_date,
            )
    latest_decision = latest_portfolio_candidate_review_decision_path(output_root)
    if latest_decision is not None:
        return latest_decision.parent / "portfolio_candidate_review_package.json"
    packages = sorted(output_root.glob("*/portfolio_candidate_review_package.json"))
    if packages:
        return max(packages, key=lambda path: path.stat().st_mtime)
    return None


def _supporting_artifacts(
    *,
    config: dict[str, Any],
    as_of: date,
    candidate_path: Path | None,
    candidates_path: Path | None,
) -> dict[str, Any]:
    specs = {
        "portfolio_sensitivity": (
            "portfolio_sensitivity_dir",
            "portfolio_sensitivity_summary.json",
        ),
        "signal_calibration": ("signal_calibration_dir", "signal_calibration_summary.json"),
        "signal_ablation": ("signal_ablation_dir", "signal_ablation_summary.json"),
        "signal_snapshot": ("signal_snapshot_dir", "signal_snapshot.json"),
        "backtest_input_manifest": (
            "backtest_snapshot_dir",
            "backtest_input_manifest.json",
        ),
        "price_cache_reconcile": (
            "price_cache_reconcile_dir",
            "price_cache_reconcile_summary.json",
        ),
        "shadow_backtest": ("shadow_backtest_dir", "shadow_backtest_summary.json"),
    }
    artifacts: dict[str, str] = {}
    missing: list[str] = []
    if candidate_path is not None and candidate_path.exists():
        artifacts["recommended_portfolio_candidate"] = str(candidate_path)
    else:
        missing.append("recommended_portfolio_candidate")
    if candidates_path is not None and candidates_path.exists():
        artifacts["portfolio_candidates"] = str(candidates_path)
    else:
        missing.append("portfolio_candidates")
    for key, (root_key, file_name) in specs.items():
        path = _latest_artifact_on_or_before(_input_root(config, root_key), file_name, as_of)
        if path is None:
            missing.append(key)
        else:
            artifacts[key] = str(path)
    return {
        "supporting_artifacts": artifacts,
        "missing_supporting_artifacts": missing,
    }


def _evidence_summary(
    *,
    candidates_payload: dict[str, Any],
    supporting: dict[str, Any],
) -> dict[str, Any]:
    candidate_metadata = _mapping(candidates_payload.get("metadata"))
    data_gate = _mapping(candidates_payload.get("data_gate"))
    ranking = _mapping(candidates_payload.get("ranking"))
    promotion = _mapping(candidates_payload.get("promotion_impact"))
    artifacts = _mapping(supporting.get("supporting_artifacts"))
    signal_snapshot = _load_json_mapping(_optional_path(artifacts.get("signal_snapshot")))
    signal_metadata = _mapping(signal_snapshot.get("metadata"))
    signal_summary = _mapping(signal_snapshot.get("summary"))
    shadow = _load_json_mapping(_optional_path(artifacts.get("shadow_backtest")))
    shadow_decision = _mapping(shadow.get("promotion_decision"))
    return {
        "data_gate": data_gate.get("status", "UNKNOWN"),
        "candidate_summary_status": candidate_metadata.get("status", "UNKNOWN"),
        "best_profile": ranking.get("best_profile", ""),
        "signal_snapshot_status": signal_metadata.get(
            "status",
            signal_summary.get("status", "UNKNOWN"),
        ),
        "promotion_status": shadow_decision.get("status", "UNKNOWN"),
        "can_support_candidate_promotion": promotion.get(
            "can_support_candidate_promotion",
            False,
        ),
    }


def _manual_review_checklist(
    *,
    evidence: dict[str, Any],
    production_modified: bool,
    hard_rejections: list[str],
) -> list[dict[str, str]]:
    return [
        {
            "item": "Confirm data gate is OK",
            "status": "PASS" if evidence.get("data_gate") == "OK" else "FAIL",
            "reason": f"data_gate={evidence.get('data_gate', 'UNKNOWN')}",
        },
        {
            "item": "Confirm signal snapshot quality is not OK",
            "status": "WARN" if evidence.get("signal_snapshot_status") == "LIMITED" else "PASS",
            "reason": f"signal_snapshot_status={evidence.get('signal_snapshot_status', 'UNKNOWN')}",
        },
        {
            "item": "Confirm production/current.yaml is unchanged",
            "status": "PASS" if not production_modified else "FAIL",
            "reason": f"production_config_modified={production_modified}",
        },
        {
            "item": "Confirm candidate is advisory only",
            "status": "PASS" if not hard_rejections else "WARN",
            "reason": "; ".join(hard_rejections),
        },
    ]


def _hard_rejections(package_like: dict[str, Any], config: dict[str, Any]) -> list[str]:
    evidence = _mapping(package_like.get("evidence_summary"))
    metadata = _mapping(package_like.get("metadata"))
    current_production = _mapping(package_like.get("current_production"))
    candidate = _mapping(package_like.get("candidate"))
    missing = {str(item) for item in _records(package_like.get("missing_supporting_artifacts"))}
    hard: list[str] = []
    if evidence.get("data_gate") not in {"OK", None}:
        hard.append("data_gate_not_ok")
    if bool(current_production.get("modified")):
        hard.append("production_config_modified")
    if not candidate.get("source_artifact") or "recommended_portfolio_candidate" in missing:
        hard.append("missing_candidate_artifact")
    if "portfolio_candidates" in missing:
        hard.append("missing_portfolio_candidates_summary")
    if metadata.get("auto_promotion") is True:
        hard.append("auto_promotion_true")
    if metadata.get("production_effect") not in {"none", None}:
        hard.append("production_effect_not_none")
    allowed = set(_records(_mapping(config.get("decision_rules")).get("hard_rejection")))
    return [item for item in hard if not allowed or item in allowed]


def _recommended_next_step(
    evidence: dict[str, Any],
    hard_rejections: list[str],
    config: dict[str, Any],
) -> str:
    if hard_rejections:
        return "rejected"
    default_recommendation = str(
        _mapping(config.get("decision_rules")).get("default_recommendation_when_limited")
        or "watch"
    )
    if (
        evidence.get("data_gate") == "OK"
        and evidence.get("candidate_summary_status") == "LIMITED"
        and evidence.get("best_profile")
        and evidence.get("signal_snapshot_status") == "LIMITED"
    ):
        return default_recommendation
    return "needs_more_data"


def _default_reason(
    decision: str,
    hard_rejections: list[str],
    config: dict[str, Any],
) -> str:
    if hard_rejections:
        return f"Hard rejection rules are present: {', '.join(hard_rejections)}."
    default = _mapping(config.get("default_decision"))
    if decision == "pending_review":
        return str(default.get("reason") or "Review package generated; awaiting decision.")
    if decision == "watch":
        return (
            "Candidate improves portfolio responsiveness, but signal snapshot quality "
            "remains LIMITED; continue observing."
        )
    if decision == "approved_for_shadow_candidate":
        return (
            "Manual review approved the portfolio profile for shadow tracking only. "
            "Production promotion remains disabled."
        )
    if decision == "needs_more_data":
        return "More supporting evidence is required before shadow tracking approval."
    return "Candidate rejected by manual review."


def _candidate_profile_name(
    candidate_payload: dict[str, Any],
    candidates_payload: dict[str, Any],
) -> str:
    return str(
        candidate_payload.get("profile_name")
        or _mapping(candidates_payload.get("ranking")).get("best_profile")
        or ""
    )


def _candidate_version(as_of: date, profile_name: str, candidate_sha: str) -> str:
    suffix = candidate_sha[:12] if candidate_sha else "missing"
    profile = profile_name or "missing"
    return f"portfolio-candidate-{as_of.isoformat()}-{profile}-{suffix}"


def _recommended_by(summary_path: Path | None) -> str:
    if summary_path is None:
        return ""
    return summary_path.name if summary_path.exists() else ""


def _fallback_signals_present(supporting: dict[str, Any]) -> bool:
    artifacts = _mapping(supporting.get("supporting_artifacts"))
    signal_snapshot = _load_json_mapping(_optional_path(artifacts.get("signal_snapshot")))
    metadata = _mapping(signal_snapshot.get("metadata"))
    summary = _mapping(signal_snapshot.get("summary"))
    if metadata.get("status") == "LIMITED" or summary.get("status") == "LIMITED":
        return True
    for key in ("fallback_signals", "proxy_signals", "neutral_fallback_signals"):
        value = summary.get(key)
        if isinstance(value, list) and value:
            return True
        if isinstance(value, int) and value > 0:
            return True
    return False


def _output_root(config: dict[str, Any]) -> Path:
    output = _mapping(config.get("output"))
    raw_root = output.get("portfolio_candidate_reviews_dir")
    return resolve_project_path(
        str(raw_root or default_portfolio_candidate_reviews_root())
    )


def _input_root(config: dict[str, Any], key: str) -> Path:
    input_config = _mapping(config.get("input"))
    default_map = {
        "portfolio_candidates_dir": PROJECT_ROOT / "artifacts" / "portfolio_candidates",
        "portfolio_sensitivity_dir": PROJECT_ROOT / "artifacts" / "portfolio_sensitivity",
        "signal_calibration_dir": PROJECT_ROOT / "artifacts" / "signal_calibration",
        "signal_ablation_dir": PROJECT_ROOT / "artifacts" / "signal_ablation",
        "signal_snapshot_dir": PROJECT_ROOT / "artifacts" / "signal_snapshots",
        "backtest_snapshot_dir": PROJECT_ROOT / "artifacts" / "backtest_snapshots",
        "price_cache_reconcile_dir": PROJECT_ROOT / "artifacts" / "data_quality",
        "shadow_backtest_dir": PROJECT_ROOT / "artifacts" / "shadow_backtest",
    }
    value = input_config.get(key)
    return resolve_project_path(str(value)) if value else default_map[key]


def _production_parameters_path(config: dict[str, Any]) -> Path:
    input_config = _mapping(config.get("input"))
    raw_path = input_config.get("production_parameters_path")
    return resolve_project_path(
        str(raw_path or PROJECT_ROOT / "config" / "parameters" / "production" / "current.yaml")
    )


def _latest_artifact_on_or_before(root: Path, file_name: str, as_of: date) -> Path | None:
    exact = root / as_of.isoformat() / file_name
    if exact.exists():
        return exact
    candidates: list[tuple[date, Path]] = []
    for path in root.glob(f"*/{file_name}"):
        try:
            candidate_date = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        if candidate_date <= as_of:
            candidates.append((candidate_date, path))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1].stat().st_mtime))[1]


def _load_json_mapping(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_yaml_mapping(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _sha256_if_exists(path: Path | None) -> str:
    if path is None or not path.exists() or not path.is_file():
        return ""
    return sha256_file(path)


def _git_path_modified(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        relative = path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError:
        return False
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain", "--", str(relative)],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return False
    return bool(result.stdout.strip())


def _validate_review_config(config: dict[str, Any]) -> None:
    if config.get("production_effect") != "none":
        raise ValueError("portfolio candidate review production_effect must be none")
    if config.get("manual_review_required") is not True:
        raise ValueError("portfolio candidate review manual_review_required must be true")
    if config.get("auto_promotion") is not False:
        raise ValueError("portfolio candidate review auto_promotion must be false")
    if _mapping(config.get("safety")).get("production_write_allowed") is not False:
        raise ValueError("portfolio candidate review production writes must be disabled")
    statuses = set(_review_statuses(config))
    if not REVIEW_STATUSES.issubset(statuses):
        raise ValueError("portfolio candidate review config missing review statuses")


def _review_statuses(config: dict[str, Any]) -> list[str]:
    return [str(item) for item in _records(config.get("review_status"))]


def _parent_date(path: Path) -> date | None:
    try:
        return date.fromisoformat(path.parent.name)
    except ValueError:
        return None


def _optional_path(value: object) -> Path | None:
    if not value:
        return None
    return Path(str(value))


def _path_text(path: Path | None) -> str:
    return "" if path is None else str(path)


def _mapping(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _records(value: object) -> list[Any]:
    return value if isinstance(value, list) else []
