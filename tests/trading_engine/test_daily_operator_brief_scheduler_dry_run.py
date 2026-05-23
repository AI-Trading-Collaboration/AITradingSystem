from __future__ import annotations

import builtins
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_dry_run import (
    render_daily_operator_brief_scheduler_dry_run_markdown,
    write_daily_operator_brief_scheduler_dry_run,
)


def test_scheduler_dry_run_all_inputs_found_is_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)
    _write_pipeline_health(data_root, as_of)
    _write_data_freshness(data_root, as_of)
    _write_operator_brief(data_root, as_of)

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "READY"
    assert payload["dry_run_status"] == "OK"
    assert payload["summary_level"] == "NORMAL"
    assert payload["safe_for_scheduled_generation"] is True
    assert payload["dependency_check"]["status"] == "PASS"
    assert payload["dependency_check"]["required_inputs_available"] is True
    assert payload["dependency_check"]["optional_inputs_available"] is True
    assert payload["safety_check"]["status"] == "PASS"
    assert payload["expected_operator_brief_behavior"]["expected_degradation"] == "NONE"
    assert payload["schedule_plan"]["expected_run_time_local"] == "09:00"
    outputs = payload["output_artifacts"]
    assert Path(outputs["json"]["path"]).exists()
    assert Path(outputs["markdown"]["path"]).exists()
    assert Path(outputs["run_log_json"]["path"]).exists()
    assert Path(outputs["run_log_markdown"]["path"]).exists()


def test_scheduler_dry_run_missing_optional_inputs_is_ready_with_warnings(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "READY_WITH_WARNINGS"
    assert payload["dry_run_status"] == "WATCH"
    assert payload["dependency_check"]["missing_required_inputs"] == []
    assert payload["dependency_check"]["missing_optional_inputs"] == [
        "pipeline_health_summary",
        "data_freshness_summary",
    ]
    assert payload["expected_operator_brief_behavior"]["expected_degradation"] == "WATCH"


def test_scheduler_dry_run_missing_digest_is_not_ready(tmp_path: Path) -> None:
    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=date(2026, 5, 23),
        data_root=tmp_path / "data",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "NOT_READY"
    assert payload["dry_run_status"] == "ACTION_REQUIRED"
    assert payload["dependency_check"]["missing_required_inputs"] == [
        "parameter_governance_daily_digest"
    ]
    assert payload["safe_for_scheduled_generation"] is False


def test_scheduler_dry_run_stale_digest_is_not_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, date(2026, 5, 20), payload_date=date(2026, 5, 20))

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        lookback_days=7,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "NOT_READY"
    assert payload["dry_run_status"] == "ACTION_REQUIRED"
    assert payload["dependency_check"]["stale_inputs"] == ["parameter_governance_daily_digest"]


@pytest.mark.parametrize(
    ("writer_name", "expected_stale"),
    [
        ("pipeline_health", "pipeline_health_summary"),
        ("data_freshness", "data_freshness_summary"),
    ],
)
def test_scheduler_dry_run_stale_optional_input_is_ready_with_warnings(
    tmp_path: Path,
    writer_name: str,
    expected_stale: str,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)
    writer = _input_writers()[writer_name]
    writer(data_root, date(2026, 5, 20), payload_date=date(2026, 5, 20))

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        lookback_days=7,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "READY_WITH_WARNINGS"
    assert payload["dry_run_status"] == "WATCH"
    assert expected_stale in payload["dependency_check"]["stale_inputs"]
    assert payload["expected_operator_brief_behavior"]["expected_degradation"] == "WATCH"


def test_scheduler_dry_run_strict_missing_optional_is_not_ready(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        strict=True,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "NOT_READY"
    assert payload["dry_run_status"] == "ACTION_REQUIRED"
    assert payload["dependency_check"]["status"] == "FAIL"


@pytest.mark.parametrize(
    ("writer_name", "override"),
    [
        ("digest", {"broker_execution": True}),
        ("pipeline_health", {"pipelines_executed_by_health_check": True}),
        ("data_freshness", {"data_downloaded_by_freshness_check": True}),
        ("operator_brief", {"broker_execution": True}),
    ],
)
def test_scheduler_dry_run_input_safety_invalid_is_safety_blocked(
    tmp_path: Path,
    writer_name: str,
    override: dict[str, Any],
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)
    _write_pipeline_health(data_root, as_of)
    _write_data_freshness(data_root, as_of)
    _write_operator_brief(data_root, as_of)
    writer = _input_writers()[writer_name]
    writer(data_root, as_of, override=override)

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == "SAFETY_BLOCKED"
    assert payload["dry_run_status"] == "SAFETY_BLOCKED"
    assert payload["safety_check"]["status"] == "FAIL"
    assert payload["safety_check"]["blocking_reasons"]


def test_scheduler_dry_run_never_executes_operator_or_upstream(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    _write_digest(data_root, as_of)
    _write_pipeline_health(data_root, as_of)
    _write_data_freshness(data_root, as_of)
    original_import = builtins.__import__

    def guarded_import(
        name: str,
        globals_: dict[str, object] | None = None,
        locals_: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        blocked_module_tokens = (
            "run_parameter_governance_daily_digest",
            "run_pipeline_health_summary",
            "run_data_freshness_summary",
            "run_daily_trading_system_operator_brief",
            "ai_trading_system.trading_engine.daily_trading_system_operator_brief",
            "ai_trading_system.data.download",
            "ai_trading_system.scoring",
            "ai_trading_system.backtest",
            "ai_trading_system.trading_engine.brokers",
            "run_paper_trading_replay",
        )
        if any(token in name for token in blocked_module_tokens):
            raise AssertionError(f"scheduler dry run must not import execution path: {name}")
        return original_import(name, globals_, locals_, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    contract = payload["scheduler_contract"]
    assert contract["runs_daily_digest_script"] is False
    assert contract["runs_pipeline_health_summary_script"] is False
    assert contract["runs_data_freshness_summary_script"] is False
    assert contract["runs_operator_brief_script"] is False
    assert contract["creates_windows_task_scheduler_task"] is False
    assert contract["runs_data_download"] is False
    assert contract["runs_broker_runner"] is False
    assert contract["runs_replay_runner"] is False
    assert contract["triggers_trade"] is False


@pytest.mark.parametrize(
    ("setup", "expected_decision", "expected_banner"),
    [
        (
            "ready",
            "READY",
            None,
        ),
        (
            "warnings",
            "READY_WITH_WARNINGS",
            "## Scheduler Dry Run Ready With Warnings",
        ),
        (
            "not_ready",
            "NOT_READY",
            "## Scheduler Dry Run Not Ready",
        ),
        (
            "safety",
            "SAFETY_BLOCKED",
            "## Scheduler Dry Run Safety Blocked",
        ),
    ],
)
def test_scheduler_dry_run_markdown_sections_and_banners(
    tmp_path: Path,
    setup: str,
    expected_decision: str,
    expected_banner: str | None,
) -> None:
    data_root = tmp_path / "data"
    as_of = date(2026, 5, 23)
    if setup in {"ready", "warnings", "safety"}:
        _write_digest(data_root, as_of, override={"broker_execution": setup == "safety"})
    if setup == "ready":
        _write_pipeline_health(data_root, as_of)
        _write_data_freshness(data_root, as_of)

    payload = write_daily_operator_brief_scheduler_dry_run(
        as_of=as_of,
        data_root=data_root,
        generated_at=_fixed_generated_at(),
    )
    markdown = render_daily_operator_brief_scheduler_dry_run_markdown(payload)

    _assert_invariants(payload)
    assert payload["dry_run_decision"] == expected_decision
    assert "## 1. Dry Run Summary" in markdown
    assert "## 3. Dependency Check" in markdown
    assert "## 4. Safety Check" in markdown
    if expected_banner is None:
        assert "## Scheduler Dry Run" not in markdown
    else:
        assert expected_banner in markdown


def _write_digest(
    data_root: Path,
    as_of: date,
    *,
    payload_date: date | None = None,
    override: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "weight_iterations"
        / "governance"
        / "digests"
        / f"parameter_governance_daily_digest_{as_of.isoformat()}.json"
    )
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "parameter_governance_daily_digest",
        "task_id": "TRADING-021",
        "date": (payload_date or as_of).isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "digest_only": True,
        "governance_only": True,
        "apply_executed_by_digest": False,
        "rollback_executed_by_digest": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "digest_status": "OK",
        "summary_level": "NORMAL",
        "headline": "Parameter governance digest is stable.",
    }
    payload.update(override or {})
    _write_json(path, payload)
    return path


def _write_pipeline_health(
    data_root: Path,
    as_of: date,
    *,
    payload_date: date | None = None,
    override: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "pipeline_health"
        / f"pipeline_health_summary_{as_of.isoformat()}.json"
    )
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "pipeline_health_summary",
        "task_id": "TRADING-023",
        "date": (payload_date or as_of).isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "pipeline_health_only": True,
        "read_only": True,
        "pipelines_executed_by_health_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "health_status": "OK",
        "summary_level": "NORMAL",
    }
    payload.update(override or {})
    _write_json(path, payload)
    return path


def _write_data_freshness(
    data_root: Path,
    as_of: date,
    *,
    payload_date: date | None = None,
    override: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "data_freshness"
        / f"data_freshness_summary_{as_of.isoformat()}.json"
    )
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "data_freshness_summary",
        "task_id": "TRADING-024",
        "date": (payload_date or as_of).isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "data_freshness_only": True,
        "read_only": True,
        "data_downloaded_by_freshness_check": False,
        "pipelines_executed_by_freshness_check": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "freshness_status": "OK",
        "summary_level": "NORMAL",
    }
    payload.update(override or {})
    _write_json(path, payload)
    return path


def _write_operator_brief(
    data_root: Path,
    as_of: date,
    *,
    payload_date: date | None = None,
    override: dict[str, Any] | None = None,
) -> Path:
    path = (
        data_root
        / "derived"
        / "operator_briefs"
        / f"daily_trading_system_operator_brief_{as_of.isoformat()}.json"
    )
    payload: dict[str, Any] = {
        "schema_version": "1.0",
        "report_type": "daily_trading_system_operator_brief",
        "task_id": "TRADING-022",
        "date": (payload_date or as_of).isoformat(),
        "production_effect": "none",
        "manual_review_only": True,
        "operator_brief_only": True,
        "read_only": True,
        "apply_executed_by_operator_brief": False,
        "rollback_executed_by_operator_brief": False,
        "broker_execution": False,
        "replay_execution": False,
        "trading_execution": False,
        "brief_status": "OK",
        "summary_level": "NORMAL",
    }
    payload.update(override or {})
    _write_json(path, payload)
    return path


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _input_writers() -> dict[str, Any]:
    return {
        "digest": _write_digest,
        "pipeline_health": _write_pipeline_health,
        "data_freshness": _write_data_freshness,
        "operator_brief": _write_operator_brief,
    }


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 23, 0, 0, tzinfo=UTC)


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["scheduler_dry_run_only"] is True
    assert payload["read_only"] is True
    assert payload["scheduler_created"] is False
    assert payload["operator_brief_executed_by_scheduler_dry_run"] is False
    assert payload["pipelines_executed_by_scheduler_dry_run"] is False
    assert payload["data_downloaded_by_scheduler_dry_run"] is False
    assert payload["apply_executed_by_scheduler_dry_run"] is False
    assert payload["rollback_executed_by_scheduler_dry_run"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
