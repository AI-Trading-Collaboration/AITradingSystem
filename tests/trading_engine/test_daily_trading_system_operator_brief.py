from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.daily_trading_system_operator_brief import (
    build_daily_trading_system_operator_brief_payload,
    render_daily_trading_system_operator_brief_markdown,
    write_daily_trading_system_operator_brief,
)


@pytest.mark.parametrize(
    ("digest_status", "expected_brief_status", "expected_summary_level"),
    [
        ("OK", "OK", "NORMAL"),
        ("WATCH", "WATCH", "WATCH"),
        ("ACTION_REQUIRED", "ACTION_REQUIRED", "ACTION"),
        ("URGENT", "URGENT", "URGENT"),
    ],
)
def test_operator_brief_status_mapping_from_digest(
    tmp_path: Path,
    digest_status: str,
    expected_brief_status: str,
    expected_summary_level: str,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context, digest_status=digest_status)
    _write_pipeline_health_summary(context)
    _write_data_freshness_summary(context)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == expected_brief_status
    assert payload["summary_level"] == expected_summary_level


def test_operator_brief_missing_digest_is_input_missing(tmp_path: Path) -> None:
    context = _write_context(tmp_path)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "INPUT_MISSING"
    assert payload["summary_level"] == "UNKNOWN"


def test_operator_brief_invalid_json_is_input_invalid(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    context["digest_path"].parent.mkdir(parents=True, exist_ok=True)
    context["digest_path"].write_text("{not json", encoding="utf-8")

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "INPUT_INVALID"


def test_operator_brief_invalid_digest_task_id_is_input_invalid(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_digest(context, overrides={"task_id": "TRADING-020"})

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "INPUT_INVALID"
    assert payload["safety_validation"]["digest_task_id_valid"] is False


@pytest.mark.parametrize(
    "overrides",
    [
        {"production_effect": "profile_updated"},
        {"broker_execution": True},
        {"replay_execution": True},
        {"trading_execution": True},
    ],
)
def test_operator_brief_blocks_invalid_digest_safety_fields(
    tmp_path: Path,
    overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context, overrides=overrides)

    payload = _build_brief(context)
    markdown = render_daily_trading_system_operator_brief_markdown(payload)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["status"] == "FAIL"
    assert "Operator Brief Safety Blocked" in markdown


def test_operator_brief_health_and_freshness_missing_degrade_to_watch(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)

    payload = _build_brief(context)
    markdown = render_daily_trading_system_operator_brief_markdown(payload)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "WATCH"
    assert payload["pipeline_health"]["status"] == "UNKNOWN"
    assert payload["data_freshness"]["status"] == "UNKNOWN"
    assert payload["market_report_status"]["status"] == "UNKNOWN"
    assert payload["pipeline_health"]["available"] is False
    assert payload["data_freshness"]["available"] is False
    assert "No TRADING-023 pipeline health summary artifact was found." in (
        payload["pipeline_health"]["notes"]
    )
    assert "No TRADING-024 data freshness summary artifact was found." in (
        payload["data_freshness"]["notes"]
    )
    assert "Status: `UNKNOWN`" in markdown
    assert "## Watch: Monitoring Recommended" in markdown
    assert "failure" not in markdown.lower()


def test_operator_brief_health_and_freshness_summaries_are_consumed(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context, warning_pipelines=1)
    _write_data_freshness_summary(context, warning_sources=1)
    reports = tmp_path / "outputs" / "reports"
    _write_json(
        reports / "market_report_2026-05-23.json",
        {
            "status": "PASS",
            "latest_report_path": str(reports / "daily_score_2026-05-23.md"),
            "notes": ["market report test fixture"],
        },
    )

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "OK"
    assert payload["input_artifacts"]["pipeline_health_summary"]["status"] == "FOUND"
    assert payload["input_artifacts"]["data_freshness_summary"]["status"] == "FOUND"
    assert payload["pipeline_health"]["status"] == "OK"
    assert payload["pipeline_health"]["health_status"] == "OK"
    assert payload["pipeline_health"]["required_pipelines"] == 8
    assert payload["pipeline_health"]["warning_pipelines"] == 1
    assert payload["data_freshness"]["status"] == "OK"
    assert payload["data_freshness"]["freshness_status"] == "OK"
    assert payload["data_freshness"]["required_sources"] == 3
    assert payload["data_freshness"]["warning_sources"] == 1
    assert payload["market_report_status"]["status"] == "PASS"


@pytest.mark.parametrize(
    ("health_status", "expected_brief_status"),
    [
        ("CRITICAL", "URGENT"),
        ("ACTION_REQUIRED", "ACTION_REQUIRED"),
        ("INCOMPLETE", "ACTION_REQUIRED"),
        ("WATCH", "WATCH"),
    ],
)
def test_operator_brief_pipeline_health_status_affects_brief_status(
    tmp_path: Path,
    health_status: str,
    expected_brief_status: str,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context, health_status=health_status)
    _write_data_freshness_summary(context)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == expected_brief_status
    assert payload["pipeline_health"]["health_status"] == health_status


@pytest.mark.parametrize(
    ("freshness_status", "expected_brief_status"),
    [
        ("CRITICAL", "URGENT"),
        ("STALE", "ACTION_REQUIRED"),
        ("MISSING", "ACTION_REQUIRED"),
        ("WATCH", "WATCH"),
    ],
)
def test_operator_brief_data_freshness_status_affects_brief_status(
    tmp_path: Path,
    freshness_status: str,
    expected_brief_status: str,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context)
    _write_data_freshness_summary(context, freshness_status=freshness_status)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == expected_brief_status
    assert payload["data_freshness"]["freshness_status"] == freshness_status


@pytest.mark.parametrize(
    "overrides",
    [
        {"broker_execution": True},
        {"pipelines_executed_by_health_check": True},
        {"production_effect": "profile_updated"},
    ],
)
def test_operator_brief_blocks_invalid_pipeline_health_safety_fields(
    tmp_path: Path,
    overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context, overrides=overrides)
    _write_data_freshness_summary(context)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["pipeline_health"]["status"] == "FAIL"


@pytest.mark.parametrize(
    "overrides",
    [
        {"data_downloaded_by_freshness_check": True},
        {"pipelines_executed_by_freshness_check": True},
        {"broker_execution": True},
    ],
)
def test_operator_brief_blocks_invalid_data_freshness_safety_fields(
    tmp_path: Path,
    overrides: dict[str, Any],
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context)
    _write_data_freshness_summary(context, overrides=overrides)

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["data_freshness"]["status"] == "FAIL"


def test_operator_brief_merges_summary_alerts_with_source_prefixes(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(
        context,
        health_status="CRITICAL",
        alerts={"critical": ["critical pipeline fixture"], "warnings": [], "notes": []},
    )
    _write_data_freshness_summary(
        context,
        freshness_status="WATCH",
        alerts={"critical": [], "warnings": ["freshness warning fixture"], "notes": []},
    )

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "URGENT"
    assert "[TRADING-023] critical pipeline fixture" in payload["alerts"]["critical"]
    assert "[TRADING-024] freshness warning fixture" in payload["alerts"]["warnings"]


def test_operator_brief_pending_manual_actions_from_health_and_freshness(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(
        context,
        overrides={
            "pending_items": {
                "pending_proposal_review": False,
                "pending_preflight": False,
                "pending_apply": True,
                "pending_rollback": False,
                "pending_lifecycle_audit": False,
            }
        },
    )
    _write_pipeline_health_summary(
        context,
        health_status="CRITICAL",
        critical_pipelines=[
            {
                "pipeline_id": "TRADING-021",
                "name": "Digest",
                "status": "CRITICAL",
                "reason": "Safety field mismatch.",
            }
        ],
    )
    _write_data_freshness_summary(
        context,
        freshness_status="STALE",
        stale_sources=[
            {
                "source_id": "parameter_governance_digest",
                "name": "Parameter Governance Digest",
                "status": "STALE",
                "reason": "Required source parameter_governance_digest is stale.",
            }
        ],
    )

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["pending_manual_actions"]["has_pending_actions"] is True
    actions = [item["action"] for item in payload["pending_manual_actions"]["items"]]
    assert "Review pending apply" in actions
    assert "Review critical pipeline health finding" in actions
    assert "Review stale required data source" in actions


def test_operator_brief_pending_manual_actions_from_digest(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_digest(
        context,
        digest_status="ACTION_REQUIRED",
        overrides={
            "governance_snapshot": {
                "governance_state": "PREFLIGHT_READY",
                "action_required": True,
                "action_level": "APPROVAL_REQUIRED",
                "recommended_action": "Review pending apply.",
                "safety_boundary_status": "PASS",
            },
            "pending_items": {
                "pending_proposal_review": False,
                "pending_preflight": False,
                "pending_apply": True,
                "pending_rollback": False,
                "pending_lifecycle_audit": False,
            },
        },
    )

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "ACTION_REQUIRED"
    assert payload["pending_manual_actions"]["has_pending_actions"] is True
    actions = [item["action"] for item in payload["pending_manual_actions"]["items"]]
    assert "Review pending apply" in actions


def test_operator_brief_critical_findings_create_urgent_manual_action(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(
        context,
        digest_status="URGENT",
        overrides={
            "alerts": {
                "critical": ["critical safety finding"],
                "warnings": [],
                "notes": [],
            }
        },
    )

    payload = _build_brief(context)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "URGENT"
    assert payload["pending_manual_actions"]["has_pending_actions"] is True
    assert any(
        item["action"] == "Urgent review of critical findings"
        for item in payload["pending_manual_actions"]["items"]
    )


def test_operator_brief_markdown_ok_contains_daily_sections(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    _write_pipeline_health_summary(context)
    _write_data_freshness_summary(context)

    payload = _build_brief(context)
    markdown = render_daily_trading_system_operator_brief_markdown(payload)

    _assert_operator_invariants(payload)
    assert "Executive Summary" in markdown
    assert "Parameter Governance" in markdown
    assert "Pipeline Health" in markdown
    assert "Data Freshness" in markdown
    assert "Recommended Next Steps" in markdown
    assert "Continue observation." in markdown


def test_operator_brief_markdown_banners(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_digest(context, digest_status="ACTION_REQUIRED")
    action = _build_brief(context)
    assert "## Action Required" in render_daily_trading_system_operator_brief_markdown(action)

    _write_digest(
        context,
        digest_status="URGENT",
        overrides={"alerts": {"critical": ["critical finding"], "warnings": [], "notes": []}},
    )
    urgent = _build_brief(context)
    urgent_markdown = render_daily_trading_system_operator_brief_markdown(urgent)
    assert "URGENT: Manual Attention Required" in urgent_markdown

    _write_digest(context, overrides={"broker_execution": True})
    safety = _build_brief(context)
    assert "Operator Brief Safety Blocked" in render_daily_trading_system_operator_brief_markdown(
        safety
    )
    _assert_operator_invariants(action)
    _assert_operator_invariants(urgent)
    _assert_operator_invariants(safety)


def test_operator_brief_writes_outputs_and_run_log(tmp_path: Path) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)

    payload = write_daily_trading_system_operator_brief(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )

    _assert_operator_invariants(payload)
    assert Path(payload["output_artifacts"]["json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["markdown"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log_json"]["path"]).exists()
    assert Path(payload["output_artifacts"]["run_log_markdown"]["path"]).exists()


def _build_brief(context: dict[str, Any]) -> dict[str, Any]:
    payload = build_daily_trading_system_operator_brief_payload(
        as_of=context["as_of"],
        data_root=context["data_root"],
        generated_at=_fixed_generated_at(),
    )
    _assert_operator_invariants(payload)
    return payload


def _write_context(tmp_path: Path) -> dict[str, Any]:
    as_of = date(2026, 5, 23)
    data_root = tmp_path / "data"
    digest_path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "governance"
        / "digests"
        / f"parameter_governance_daily_digest_{as_of.isoformat()}.json"
    )
    return {
        "as_of": as_of,
        "data_root": data_root,
        "digest_path": digest_path,
    }


def _write_digest(
    context: dict[str, Any],
    *,
    digest_status: str = "OK",
    overrides: dict[str, Any] | None = None,
) -> None:
    payload = _valid_digest(context, digest_status=digest_status)
    payload.update(overrides or {})
    _write_json(context["digest_path"], payload)


def _write_pipeline_health_summary(
    context: dict[str, Any],
    *,
    health_status: str = "OK",
    warning_pipelines: int = 0,
    critical_pipelines: list[dict[str, Any]] | None = None,
    alerts: dict[str, list[str]] | None = None,
    overrides: dict[str, Any] | None = None,
) -> None:
    suffix = context["as_of"].isoformat()
    path = (
        context["data_root"]
        / "derived"
        / "pipeline_health"
        / f"pipeline_health_summary_{suffix}.json"
    )
    markdown_path = path.with_suffix(".md")
    critical = critical_pipelines or []
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "pipeline_health_summary",
        "task_id": "TRADING-023",
        "date": suffix,
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "pipeline_health_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "pipelines_executed_by_health_check": False,
        "apply_executed_by_health_check": False,
        "rollback_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "health_status": health_status,
        "summary_level": {
            "OK": "NORMAL",
            "WATCH": "WATCH",
            "ACTION_REQUIRED": "ACTION",
            "INCOMPLETE": "ACTION",
            "CRITICAL": "CRITICAL",
        }.get(health_status, "UNKNOWN"),
        "headline": "Required pipeline artifacts are available.",
        "coverage": {
            "registered_pipelines": 8,
            "required_pipelines": 8,
            "available_pipelines": 8,
            "missing_required_pipelines": 1 if health_status == "INCOMPLETE" else 0,
            "stale_required_pipelines": 1 if health_status == "ACTION_REQUIRED" else 0,
            "critical_pipelines": len(critical) or (1 if health_status == "CRITICAL" else 0),
            "warning_pipelines": warning_pipelines,
        },
        "pipeline_results": (
            [
                {
                    "pipeline_id": "TRADING-021",
                    "name": "Parameter Governance Digest",
                    "required": True,
                    "status": "ACTION_REQUIRED",
                    "reason": "Required pipeline issue.",
                }
            ]
            if health_status == "ACTION_REQUIRED"
            else []
        ),
        "missing_required_pipelines": (
            [
                {
                    "pipeline_id": "TRADING-022",
                    "name": "Operator Brief",
                    "status": "MISSING",
                    "reason": "Required artifact is missing.",
                }
            ]
            if health_status == "INCOMPLETE"
            else []
        ),
        "stale_pipelines": (
            [
                {
                    "pipeline_id": "TRADING-021",
                    "name": "Parameter Governance Digest",
                    "status": "STALE",
                    "reason": "Required artifact is stale.",
                }
            ]
            if health_status == "ACTION_REQUIRED"
            else []
        ),
        "critical_pipelines": critical
        or (
            [
                {
                    "pipeline_id": "TRADING-021",
                    "name": "Parameter Governance Digest",
                    "status": "CRITICAL",
                    "reason": "Critical pipeline condition detected.",
                }
            ]
            if health_status == "CRITICAL"
            else []
        ),
        "warning_pipelines": [],
        "operator_brief_integration": {"notes": ["TRADING-022 consumed this summary."]},
        "alerts": alerts or {"critical": [], "warnings": [], "notes": []},
        "output_artifacts": {
            "json": {"path": str(path)},
            "markdown": {"path": str(markdown_path)},
        },
    }
    payload.update(overrides or {})
    _write_json(path, payload)
    markdown_path.write_text("# Pipeline Health Summary\n", encoding="utf-8")


def _write_data_freshness_summary(
    context: dict[str, Any],
    *,
    freshness_status: str = "OK",
    warning_sources: int = 0,
    stale_sources: list[dict[str, Any]] | None = None,
    alerts: dict[str, list[str]] | None = None,
    overrides: dict[str, Any] | None = None,
) -> None:
    suffix = context["as_of"].isoformat()
    path = (
        context["data_root"]
        / "derived"
        / "data_freshness"
        / f"data_freshness_summary_{suffix}.json"
    )
    markdown_path = path.with_suffix(".md")
    stale = stale_sources or []
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "data_freshness_summary",
        "task_id": "TRADING-024",
        "date": suffix,
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "data_freshness_summary_only",
        "production_effect": "none",
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "safe_for_scheduler": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "apply_executed_by_freshness_check": False,
        "rollback_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "freshness_status": freshness_status,
        "summary_level": {
            "OK": "NORMAL",
            "WATCH": "WATCH",
            "STALE": "ACTION",
            "MISSING": "ACTION",
            "CRITICAL": "CRITICAL",
        }.get(freshness_status, "UNKNOWN"),
        "headline": "Required data sources are fresh enough.",
        "coverage": {
            "registered_sources": 3,
            "required_sources": 3,
            "available_sources": 3,
            "missing_required_sources": 1 if freshness_status == "MISSING" else 0,
            "stale_required_sources": len(stale) or (1 if freshness_status == "STALE" else 0),
            "critical_sources": 1 if freshness_status == "CRITICAL" else 0,
            "warning_sources": warning_sources,
        },
        "source_results": [],
        "missing_required_sources": (
            [
                {
                    "source_id": "parameter_governance_digest",
                    "name": "Parameter Governance Digest",
                    "status": "MISSING",
                    "reason": "Required source is missing.",
                }
            ]
            if freshness_status == "MISSING"
            else []
        ),
        "stale_sources": stale
        or (
            [
                {
                    "source_id": "parameter_governance_digest",
                    "name": "Parameter Governance Digest",
                    "status": "STALE",
                    "reason": "Required source is stale.",
                }
            ]
            if freshness_status == "STALE"
            else []
        ),
        "critical_sources": (
            [
                {
                    "source_id": "parameter_governance_digest",
                    "name": "Parameter Governance Digest",
                    "status": "CRITICAL",
                    "reason": "Critical data freshness condition detected.",
                }
            ]
            if freshness_status == "CRITICAL"
            else []
        ),
        "warning_sources": [],
        "operator_brief_integration": {"notes": ["TRADING-022 consumed this summary."]},
        "alerts": alerts or {"critical": [], "warnings": [], "notes": []},
        "output_artifacts": {
            "json": {"path": str(path)},
            "markdown": {"path": str(markdown_path)},
        },
    }
    payload.update(overrides or {})
    _write_json(path, payload)
    markdown_path.write_text("# Data Freshness Summary\n", encoding="utf-8")


def _valid_digest(context: dict[str, Any], *, digest_status: str) -> dict[str, Any]:
    suffix = context["as_of"].isoformat()
    summary_level = {
        "OK": "NORMAL",
        "WATCH": "WATCH",
        "ACTION_REQUIRED": "ACTION",
        "URGENT": "URGENT",
    }[digest_status]
    return {
        "schema_version": "1.0",
        "report_type": "parameter_governance_daily_digest",
        "task_id": "TRADING-021",
        "date": suffix,
        "generated_at": _fixed_generated_at().isoformat(),
        "mode": "parameter_governance_daily_digest_only",
        "production_effect": "none",
        "manual_review_only": True,
        "digest_only": True,
        "governance_only": True,
        "apply_executed_by_digest": False,
        "rollback_executed_by_digest": False,
        "safe_for_scheduler": True,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "digest_status": digest_status,
        "summary_level": summary_level,
        "headline": "Parameter governance is stable.",
        "governance_snapshot": {
            "governance_state": "ROLLBACK_COMPLETED",
            "action_required": digest_status in {"ACTION_REQUIRED", "URGENT"},
            "action_level": "NONE" if digest_status in {"OK", "WATCH"} else "REVIEW_REQUIRED",
            "recommended_action": "Continue observation.",
            "safety_boundary_status": "PASS",
        },
        "pending_items": {
            "pending_proposal_review": False,
            "pending_preflight": False,
            "pending_apply": False,
            "pending_rollback": False,
            "pending_lifecycle_audit": False,
        },
        "alerts": {"critical": [], "warnings": [], "notes": []},
        "output_artifacts": {
            "json": {"path": str(context["digest_path"])},
            "markdown": {"path": str(context["digest_path"].with_suffix(".md"))},
        },
        "links": {
            "daily_digest_markdown": str(context["digest_path"].with_suffix(".md")),
            "governance_web_view_html": str(
                context["data_root"]
                / "derived"
                / "weight_iterations"
                / "governance"
                / "web"
                / f"parameter_governance_web_view_{suffix}.html"
            ),
        },
    }


def _assert_operator_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["operator_brief_only"] is True
    assert payload["read_only"] is True
    assert payload["apply_executed_by_operator_brief"] is False
    assert payload["rollback_executed_by_operator_brief"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["safe_for_scheduler"] is True


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, tzinfo=UTC)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
