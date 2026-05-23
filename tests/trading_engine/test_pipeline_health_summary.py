from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.pipeline_health_summary import (
    PipelineDefinition,
    build_pipeline_health_summary_payload,
    render_pipeline_health_summary_markdown,
    write_pipeline_health_summary,
)


def test_required_artifact_found_healthy_decision_is_ok(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("TRADING-021"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"decision": "OK"})

    payload = _build(data_root, registry)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "OK"
    assert payload["coverage"]["required_pipelines"] == 1
    assert payload["pipeline_results"][0]["status"] == "HEALTHY"
    assert payload["pipeline_results"][0]["artifact_status"] == "FOUND"


def test_required_missing_is_incomplete_and_optional_missing_is_watch(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("required", required=True),
        _definition("optional", required=False, glob_name="optional"),
    )

    payload = _build(data_root, registry, include_optional_pipelines=True)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "INCOMPLETE"
    required = _result(payload, "required")
    optional = _result(payload, "optional")
    assert required["status"] == "MISSING"
    assert optional["status"] == "OPTIONAL_MISSING"
    assert payload["missing_required_pipelines"]


def test_optional_missing_only_is_watch_not_incomplete(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("optional", required=False),)

    payload = _build(data_root, registry, include_optional_pipelines=True)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "WATCH"
    assert payload["missing_required_pipelines"] == []
    assert _result(payload, "optional")["status"] == "OPTIONAL_MISSING"


def test_required_invalid_json_is_pipeline_error(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("required"),)
    path = data_root / "derived" / "pipeline_health_test" / "demo_2026-05-23.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{not json", encoding="utf-8")

    payload = _build(data_root, registry)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "ERROR"
    assert _result(payload, "required")["status"] == "ERROR"


def test_status_mapping_and_missing_status_field(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("healthy", glob_name="healthy"),
        _definition("action", glob_name="action"),
        _definition("critical", glob_name="critical"),
        _definition("unknown", glob_name="unknown"),
    )
    _write_artifact(data_root, "healthy_2026-05-23.json", {"decision": "OK"})
    _write_artifact(data_root, "action_2026-05-23.json", {"decision": "ACTION_REQUIRED"})
    _write_artifact(data_root, "critical_2026-05-23.json", {"decision": "URGENT"})
    _write_artifact(data_root, "unknown_2026-05-23.json", {"other_status": "OK"})

    payload = _build(data_root, registry)

    _assert_health_invariants(payload)
    assert _result(payload, "healthy")["status"] == "HEALTHY"
    assert _result(payload, "action")["status"] == "ACTION_REQUIRED"
    assert _result(payload, "critical")["status"] == "CRITICAL"
    unknown = _result(payload, "unknown")
    assert unknown["status"] == "UNKNOWN"
    assert "Status field not found." in unknown["warnings"]
    assert payload["health_status"] == "CRITICAL"


def test_freshness_rules_and_long_lifecycle_threshold(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("fresh", glob_name="fresh"),
        _definition("stale_required", glob_name="stale_required"),
        _definition("stale_optional", required=False, glob_name="stale_optional"),
        _definition("lifecycle", glob_name="lifecycle", stale_after_days=30),
    )
    _write_artifact(data_root, "fresh_2026-05-23.json", {"decision": "OK"})
    _write_artifact(data_root, "stale_required_2026-05-20.json", {"decision": "OK"})
    _write_artifact(data_root, "stale_optional_2026-05-20.json", {"decision": "OK"})
    _write_artifact(data_root, "lifecycle_2026-05-03.json", {"decision": "OK"})

    payload = _build(data_root, registry, include_optional_pipelines=True)

    _assert_health_invariants(payload)
    assert _result(payload, "fresh")["freshness_status"] == "FRESH"
    assert _result(payload, "stale_required")["status"] == "STALE"
    assert _result(payload, "stale_optional")["status"] == "WATCH"
    assert _result(payload, "lifecycle")["status"] == "HEALTHY"
    assert payload["health_status"] == "ACTION_REQUIRED"
    assert payload["stale_pipelines"]


def test_safety_field_scan_marks_execution_as_critical(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (
        _definition("broker", glob_name="broker"),
        _definition("replay", glob_name="replay"),
        _definition("trading", glob_name="trading"),
        _definition("apply", glob_name="apply"),
    )
    _write_artifact(
        data_root, "broker_2026-05-23.json", {"decision": "OK", "broker_execution": True}
    )
    _write_artifact(
        data_root, "replay_2026-05-23.json", {"decision": "OK", "replay_execution": True}
    )
    _write_artifact(
        data_root,
        "trading_2026-05-23.json",
        {"decision": "OK", "trading_execution": True},
    )
    _write_artifact(data_root, "apply_2026-05-23.json", {"decision": "OK", "apply_executed": True})

    payload = _build(data_root, registry)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "CRITICAL"
    assert all(
        _result(payload, item)["status"] == "CRITICAL"
        for item in ("broker", "replay", "trading", "apply")
    )
    assert payload["alerts"]["critical"]


def test_apply_and_rollback_execution_are_allowed_for_018e2_and_018e3(tmp_path: Path) -> None:
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
            "rollback_decision": "ROLLED_BACK",
            "rollback_executed": True,
            "production_effect": "profile_rolled_back_only_if_rollback_executed",
        },
    )

    payload = _build(data_root, registry)

    _assert_health_invariants(payload)
    assert payload["health_status"] == "OK"
    assert _result(payload, "TRADING-018E2")["status"] == "HEALTHY"
    assert _result(payload, "TRADING-018E3")["status"] == "HEALTHY"


def test_markdown_sections_and_banners(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("ok"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"decision": "OK"})
    ok_payload = _build(data_root, registry)
    ok_markdown = render_pipeline_health_summary_markdown(ok_payload)
    assert "Health Summary" in ok_markdown
    assert "Required Pipelines" in ok_markdown
    assert "Optional Pipelines" in ok_markdown

    _write_artifact(data_root, "demo_2026-05-23.json", {"decision": "URGENT"})
    critical_payload = _build(data_root, registry)
    assert "CRITICAL: Pipeline Health Issue Detected" in render_pipeline_health_summary_markdown(
        critical_payload
    )

    _write_artifact(data_root, "demo_2026-05-23.json", {"decision": "ACTION_REQUIRED"})
    action_payload = _build(data_root, registry)
    assert "## Action Required" in render_pipeline_health_summary_markdown(action_payload)


def test_write_pipeline_health_summary_outputs_files_and_run_log(tmp_path: Path) -> None:
    data_root = tmp_path / "data"
    registry = (_definition("ok"),)
    _write_artifact(data_root, "demo_2026-05-23.json", {"decision": "OK"})

    payload = write_pipeline_health_summary(
        as_of=date(2026, 5, 23),
        data_root=data_root,
        generated_at=datetime(2026, 5, 23, tzinfo=UTC),
        registry=registry,
    )

    _assert_health_invariants(payload)
    outputs = payload["output_artifacts"]
    assert Path(outputs["json"]["path"]).exists()
    assert Path(outputs["markdown"]["path"]).exists()
    assert Path(outputs["run_log_json"]["path"]).exists()
    assert Path(outputs["run_log_markdown"]["path"]).exists()
    run_log = json.loads(Path(outputs["run_log_json"]["path"]).read_text(encoding="utf-8"))
    assert run_log["pipeline_health_only"] is True
    assert run_log["pipelines_executed_by_health_check"] is False


def _definition(
    pipeline_id: str,
    *,
    required: bool = True,
    glob_name: str = "demo",
    status_field: str = "decision",
    healthy_values: tuple[str, ...] = ("OK",),
    allow_apply_execution: bool = False,
    allow_rollback_execution: bool = False,
    allow_promotion_execution: bool = False,
    allowed_production_effects: tuple[str, ...] = ("none",),
    stale_after_days: int | None = None,
) -> PipelineDefinition:
    return PipelineDefinition(
        pipeline_id=pipeline_id,
        name=f"{pipeline_id} Test Pipeline",
        category="test",
        required=required,
        expected_artifact_glob=f"data/derived/pipeline_health_test/{glob_name}_*.json",
        status_field=status_field,
        healthy_values=healthy_values,
        warning_values=("WATCH",),
        action_values=("ACTION_REQUIRED", "INSUFFICIENT_DATA"),
        critical_values=("URGENT", "SAFETY_BLOCKED", "ERROR"),
        allow_apply_execution=allow_apply_execution,
        allow_rollback_execution=allow_rollback_execution,
        allow_promotion_execution=allow_promotion_execution,
        allowed_production_effects=allowed_production_effects,
        stale_after_days=stale_after_days,
    )


def _write_artifact(data_root: Path, filename: str, payload: dict[str, Any]) -> Path:
    path = data_root / "derived" / "pipeline_health_test" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _build(
    data_root: Path,
    registry: tuple[PipelineDefinition, ...],
    *,
    include_optional_pipelines: bool = False,
) -> dict[str, Any]:
    return build_pipeline_health_summary_payload(
        as_of=date(2026, 5, 23),
        data_root=data_root,
        generated_at=datetime(2026, 5, 23, tzinfo=UTC),
        registry=registry,
        include_optional_pipelines=include_optional_pipelines,
    )


def _result(payload: dict[str, Any], pipeline_id: str) -> dict[str, Any]:
    return next(item for item in payload["pipeline_results"] if item["pipeline_id"] == pipeline_id)


def _assert_health_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["pipeline_health_only"] is True
    assert payload["read_only"] is True
    assert payload["pipelines_executed_by_health_check"] is False
    assert payload["apply_executed_by_health_check"] is False
    assert payload["rollback_executed_by_health_check"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
