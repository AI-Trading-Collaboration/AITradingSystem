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


def test_operator_brief_optional_artifacts_missing_do_not_block(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)

    payload = _build_brief(context)
    markdown = render_daily_trading_system_operator_brief_markdown(payload)

    _assert_operator_invariants(payload)
    assert payload["brief_status"] == "OK"
    assert payload["pipeline_health"]["status"] == "UNKNOWN"
    assert payload["data_freshness"]["status"] == "UNKNOWN"
    assert payload["market_report_status"]["status"] == "UNKNOWN"
    assert "Status: `UNKNOWN`" in markdown
    assert "failure" not in markdown.lower()


def test_operator_brief_optional_artifacts_present_are_summarized(
    tmp_path: Path,
) -> None:
    context = _write_context(tmp_path)
    _write_digest(context)
    reports = tmp_path / "outputs" / "reports"
    _write_json(
        reports / "pipeline_health_2026-05-23.json",
        {
            "status": "FAIL",
            "pipeline_runs_checked": ["score_daily"],
            "failed_pipelines": ["score_daily"],
            "stale_pipelines": ["pit_snapshots"],
            "notes": ["pipeline health test fixture"],
        },
    )
    _write_json(
        reports / "data_freshness_2026-05-23.json",
        {
            "status": "PASS",
            "fresh_sources": ["prices_daily"],
            "stale_sources": [],
            "missing_sources": [],
            "notes": ["freshness test fixture"],
        },
    )
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
    assert payload["brief_status"] == "URGENT"
    assert payload["pipeline_health"]["status"] == "FAIL"
    assert payload["pipeline_health"]["failed_pipelines"] == ["score_daily"]
    assert payload["data_freshness"]["status"] == "PASS"
    assert payload["market_report_status"]["status"] == "PASS"


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
