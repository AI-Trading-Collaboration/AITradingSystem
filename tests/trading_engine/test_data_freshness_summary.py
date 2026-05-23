from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.data_freshness_summary import (
    DataSourceDefinition,
    build_data_freshness_summary_payload,
    render_data_freshness_summary_markdown,
    write_data_freshness_summary,
)


def test_required_artifact_found_fresh_decision_is_ok(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("parameter_governance_digest"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "OK"
    result = _result(payload, "parameter_governance_digest")
    assert result["status"] == "FRESH"
    assert result["artifact_status"] == "FOUND"
    assert result["freshness_status"] == "FRESH"


def test_required_missing_and_optional_missing_statuses(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("required", required=True),
        _definition("optional", required=False, glob_name="optional"),
    )

    payload = _build(data_root, registry, include_optional_sources=True)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "MISSING"
    assert _result(payload, "required")["status"] == "MISSING"
    assert _result(payload, "optional")["status"] == "OPTIONAL_MISSING"
    assert payload["missing_required_sources"]


def test_optional_missing_only_is_watch_not_missing(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("optional", required=False),)

    payload = _build(data_root, registry, include_optional_sources=True)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "WATCH"
    assert payload["missing_required_sources"] == []
    assert _result(payload, "optional")["status"] == "OPTIONAL_MISSING"


def test_required_invalid_json_is_error(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("required"),)
    path = data_root / "derived" / "data_freshness_test" / "demo_2026-05-23.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "ERROR"
    assert _result(payload, "required")["status"] == "ERROR"


def test_status_mapping_and_missing_status_field(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("healthy", glob_name="healthy"),
        _definition("action", glob_name="action"),
        _definition("critical", glob_name="critical"),
        _definition("unknown", glob_name="unknown"),
    )
    _write_artifact(data_root, "healthy_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})
    _write_artifact(
        data_root,
        "action_2026-05-23.json",
        {"date": "2026-05-23", "decision": "ACTION_REQUIRED"},
    )
    _write_artifact(
        data_root,
        "critical_2026-05-23.json",
        {"date": "2026-05-23", "decision": "URGENT"},
    )
    _write_artifact(data_root, "unknown_2026-05-23.json", {"date": "2026-05-23"})

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert _result(payload, "healthy")["status"] == "FRESH"
    assert _result(payload, "action")["status"] == "STALE"
    assert _result(payload, "critical")["status"] == "CRITICAL"
    unknown = _result(payload, "unknown")
    assert unknown["status"] == "UNKNOWN"
    assert "Status field not found." in unknown["warnings"]
    assert payload["freshness_status"] == "CRITICAL"


def test_date_extraction_sources_and_unknown_date(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("json_date", glob_name="json_date"),
        _definition("filename_date", glob_name="filename_date"),
        _definition("modified_date", glob_name="modified_date"),
        _definition(
            "unknown_date",
            glob_name="unknown_date",
            allow_modified_time_date=False,
        ),
    )
    _write_artifact(
        data_root,
        "json_date_2026-05-22.json",
        {"date": "2026-05-23", "decision": "OK"},
    )
    _write_artifact(
        data_root,
        "filename_date_2026_05_23.json",
        {"decision": "OK"},
    )
    modified = _write_artifact(data_root, "modified_date.json", {"decision": "OK"})
    _set_mtime(modified, date(2026, 5, 23))
    unknown = _write_artifact(data_root, "unknown_date.json", {"decision": "OK"})
    _set_mtime(unknown, date(2026, 5, 23))

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert _result(payload, "json_date")["date_source"] == "json_field:date"
    assert _result(payload, "filename_date")["date_source"] == "filename"
    assert _result(payload, "modified_date")["date_source"] == "modified_time"
    unknown_result = _result(payload, "unknown_date")
    assert unknown_result["status"] == "UNKNOWN"
    assert "Unable to derive data date." in unknown_result["warnings"]


def test_freshness_rules_and_long_lifecycle_threshold(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("fresh", glob_name="fresh"),
        _definition("stale_required", glob_name="stale_required"),
        _definition("stale_optional", required=False, glob_name="stale_optional"),
        _definition("lifecycle", glob_name="lifecycle", stale_after_days=30),
    )
    _write_artifact(data_root, "fresh_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})
    _write_artifact(
        data_root,
        "stale_required_2026-05-20.json",
        {"date": "2026-05-20", "decision": "OK"},
    )
    _write_artifact(
        data_root,
        "stale_optional_2026-05-20.json",
        {"date": "2026-05-20", "decision": "OK"},
    )
    _write_artifact(
        data_root,
        "lifecycle_2026-05-03.json",
        {"date": "2026-05-03", "decision": "OK"},
    )

    payload = _build(data_root, registry, include_optional_sources=True)

    _assert_freshness_invariants(payload)
    assert _result(payload, "fresh")["status"] == "FRESH"
    assert _result(payload, "stale_required")["status"] == "STALE"
    assert _result(payload, "stale_optional")["status"] == "WATCH"
    assert _result(payload, "lifecycle")["status"] == "FRESH"
    assert payload["freshness_status"] == "STALE"
    assert payload["stale_sources"]


def test_safety_field_scan_marks_unexpected_execution_as_critical(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("broker", glob_name="broker"),
        _definition("replay", glob_name="replay"),
        _definition("trading", glob_name="trading"),
        _definition("download", glob_name="download"),
        _definition("pipeline", glob_name="pipeline"),
        _definition("apply", glob_name="apply"),
    )
    _write_artifact(
        data_root,
        "broker_2026-05-23.json",
        {"date": "2026-05-23", "decision": "OK", "broker_execution": True},
    )
    _write_artifact(
        data_root,
        "replay_2026-05-23.json",
        {"date": "2026-05-23", "decision": "OK", "replay_execution": True},
    )
    _write_artifact(
        data_root,
        "trading_2026-05-23.json",
        {"date": "2026-05-23", "decision": "OK", "trading_execution": True},
    )
    _write_artifact(
        data_root,
        "download_2026-05-23.json",
        {
            "date": "2026-05-23",
            "decision": "OK",
            "data_downloaded_by_freshness_check": True,
        },
    )
    _write_artifact(
        data_root,
        "pipeline_2026-05-23.json",
        {
            "date": "2026-05-23",
            "decision": "OK",
            "pipelines_executed_by_freshness_check": True,
        },
    )
    _write_artifact(
        data_root,
        "apply_2026-05-23.json",
        {"date": "2026-05-23", "decision": "OK", "apply_executed": True},
    )

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "CRITICAL"
    assert all(
        _result(payload, item)["status"] == "CRITICAL"
        for item in ("broker", "replay", "trading", "download", "pipeline", "apply")
    )
    assert payload["alerts"]["critical"]
    assert payload["safety_validation"]["status"] == "FAIL"


def test_apply_and_rollback_execution_are_allowed_for_018e2_and_018e3(
    tmp_path: Path,
) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition(
            "TRADING-018E2",
            glob_name="apply_result",
            healthy_values=("APPLIED",),
            allow_apply_execution=True,
            allow_promotion_execution=True,
            allowed_production_effects=("none", "profile_updated_only_if_apply_executed"),
        ),
        _definition(
            "TRADING-018E3",
            glob_name="rollback_result",
            status_field="rollback_decision",
            healthy_values=("ROLLED_BACK",),
            allow_rollback_execution=True,
            allowed_production_effects=("none", "profile_rolled_back_only_if_rollback_executed"),
        ),
    )
    _write_artifact(
        data_root,
        "apply_result_2026-05-23.json",
        {
            "date": "2026-05-23",
            "decision": "APPLIED",
            "apply_executed": True,
            "promotion_executed": True,
            "production_effect": "profile_updated_only_if_apply_executed",
        },
    )
    _write_artifact(
        data_root,
        "rollback_result_2026-05-23.json",
        {
            "date": "2026-05-23",
            "rollback_decision": "ROLLED_BACK",
            "rollback_executed": True,
            "production_effect": "profile_rolled_back_only_if_rollback_executed",
        },
    )

    payload = _build(data_root, registry)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "OK"
    assert _result(payload, "TRADING-018E2")["status"] == "FRESH"
    assert _result(payload, "TRADING-018E3")["status"] == "FRESH"


def test_overall_status_precedence(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("fresh", glob_name="fresh"),
        _definition("optional", required=False, glob_name="optional"),
    )
    _write_artifact(data_root, "fresh_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})

    payload = _build(data_root, registry, include_optional_sources=True)

    _assert_freshness_invariants(payload)
    assert payload["freshness_status"] == "WATCH"
    assert _result(payload, "fresh")["status"] == "FRESH"
    assert _result(payload, "optional")["status"] == "OPTIONAL_MISSING"


def test_markdown_sections_and_banners(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("ok"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})
    ok_payload = _build(data_root, registry)
    _assert_freshness_invariants(ok_payload)
    ok_markdown = render_data_freshness_summary_markdown(ok_payload)
    assert "Freshness Summary" in ok_markdown
    assert "Required Sources" in ok_markdown
    assert "Optional Sources" in ok_markdown

    _write_artifact(
        data_root,
        "demo_2026-05-23.json",
        {"date": "2026-05-23", "decision": "URGENT"},
    )
    critical_payload = _build(data_root, registry)
    _assert_freshness_invariants(critical_payload)
    assert "CRITICAL: Data Freshness Issue Detected" in (
        render_data_freshness_summary_markdown(critical_payload)
    )

    stale_data_root = tmp_path / "stale_parent" / "data"
    _write_artifact(
        stale_data_root,
        "demo_2026-05-20.json",
        {"date": "2026-05-20", "decision": "OK"},
    )
    stale_payload = _build(stale_data_root, registry)
    _assert_freshness_invariants(stale_payload)
    assert "Stale Required Data Detected" in render_data_freshness_summary_markdown(stale_payload)

    missing_payload = _build(tmp_path / "missing_parent" / "data", registry)
    _assert_freshness_invariants(missing_payload)
    assert "Required Data Missing" in render_data_freshness_summary_markdown(missing_payload)


def test_write_data_freshness_summary_outputs_files_and_run_log(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("ok"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"date": "2026-05-23", "decision": "OK"})

    payload = write_data_freshness_summary(
        as_of=date(2026, 5, 23),
        data_root=data_root,
        generated_at=datetime(2026, 5, 23, tzinfo=UTC),
        registry=registry,
    )

    _assert_freshness_invariants(payload)
    outputs = payload["output_artifacts"]
    assert Path(outputs["json"]["path"]).exists()
    assert Path(outputs["markdown"]["path"]).exists()
    assert Path(outputs["run_log_json"]["path"]).exists()
    assert Path(outputs["run_log_markdown"]["path"]).exists()
    run_log = json.loads(Path(outputs["run_log_json"]["path"]).read_text(encoding="utf-8"))
    assert run_log["data_freshness_only"] is True
    assert run_log["data_downloaded_by_freshness_check"] is False
    assert run_log["pipelines_executed_by_freshness_check"] is False


def _definition(
    source_id: str,
    *,
    required: bool = True,
    glob_name: str = "demo",
    status_field: str | None = "decision",
    healthy_values: tuple[str, ...] = ("OK",),
    stale_after_days: int | None = None,
    allow_apply_execution: bool = False,
    allow_rollback_execution: bool = False,
    allow_promotion_execution: bool = False,
    allowed_production_effects: tuple[str, ...] = ("none",),
    allow_modified_time_date: bool = True,
) -> DataSourceDefinition:
    return DataSourceDefinition(
        source_id=source_id,
        name=f"{source_id} Test Source",
        category="test",
        required=required,
        expected_artifact_glob=f"data/derived/data_freshness_test/{glob_name}*.json",
        date_fields=("date",),
        status_field=status_field,
        healthy_values=healthy_values,
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INSUFFICIENT_DATA"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
        stale_after_days=stale_after_days,
        allow_apply_execution=allow_apply_execution,
        allow_rollback_execution=allow_rollback_execution,
        allow_promotion_execution=allow_promotion_execution,
        allowed_production_effects=allowed_production_effects,
        allow_modified_time_date=allow_modified_time_date,
    )


def _write_artifact(data_root: Path, filename: str, payload: dict[str, Any]) -> Path:
    path = data_root / "derived" / "data_freshness_test" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _set_mtime(path: Path, as_of: date) -> None:
    timestamp = datetime(as_of.year, as_of.month, as_of.day, tzinfo=UTC).timestamp()
    os.utime(path, (timestamp, timestamp))


def _build(
    data_root: Path,
    registry: tuple[DataSourceDefinition, ...],
    *,
    include_optional_sources: bool = False,
) -> dict[str, Any]:
    return build_data_freshness_summary_payload(
        as_of=date(2026, 5, 23),
        data_root=data_root,
        generated_at=datetime(2026, 5, 23, tzinfo=UTC),
        registry=registry,
        include_optional_sources=include_optional_sources,
    )


def _result(payload: dict[str, Any], source_id: str) -> dict[str, Any]:
    return next(item for item in payload["source_results"] if item["source_id"] == source_id)


def _assert_freshness_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["data_freshness_only"] is True
    assert payload["read_only"] is True
    assert payload["data_downloaded_by_freshness_check"] is False
    assert payload["pipelines_executed_by_freshness_check"] is False
    assert payload["apply_executed_by_freshness_check"] is False
    assert payload["rollback_executed_by_freshness_check"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
