from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.cli_commands import ops as ops_cli
from ai_trading_system.ops_daily import (
    DailyOpsRunMetadata,
    DailyOpsRunReport,
    build_daily_ops_plan,
)
from ai_trading_system.run_artifacts import (
    build_run_artifact_paths,
    prepare_run_directories,
    write_run_manifest,
)

AS_OF = date(2026, 7, 23)


def _write_final_reader_brief(reports_dir: Path) -> tuple[Path, Path, bytes, bytes]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = ops_cli.default_reader_brief_json_path(reports_dir, AS_OF)
    html_path = ops_cli.default_reader_brief_html_path(reports_dir, AS_OF)
    json_bytes = json.dumps(
        {
            "schema_version": 1,
            "report_type": "reader_brief",
            "as_of": AS_OF.isoformat(),
            "status": "OK",
            "production_effect": "none",
            "payload_marker": "post-dashboard-final-bytes",
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    html_bytes = b"<html><body>post-dashboard-final-bytes</body></html>"
    json_path.write_bytes(json_bytes)
    html_path.write_bytes(html_bytes)
    ops_cli.default_report_index_json_path(reports_dir, AS_OF).write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "report_index",
                "as_of": AS_OF.isoformat(),
                "status": "PASS",
                "production_effect": "none",
                "reports": [],
                "summary": {"report_count": 0},
            }
        ),
        encoding="utf-8",
    )
    return json_path, html_path, json_bytes, html_bytes


def _report_quality_payload(status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "report_quality_gate",
        "as_of": AS_OF.isoformat(),
        "status": status,
        "report_quality_status": status,
        "production_effect": "none",
        "summary": {
            "checked_report_count": 0,
            "missing_section_count": 0,
            "blocking_quality_issue_count": 1 if status == "FAIL" else 0,
            "warning_quality_issue_count": (1 if status == "PASS_WITH_WARNINGS" else 0),
        },
        "missing_sections": [],
        "blocking_quality_issues": [],
        "warning_quality_issues": [],
        "report_checks": [],
        "reader_brief_checks": [],
    }


def _reader_quality_payload(status: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "report_type": "reader_brief_quality",
        "as_of": AS_OF.isoformat(),
        "status": status,
        "production_effect": "none",
        "summary": {
            "check_count": 1,
            "failed_check_count": 1 if status == "FAILED" else 0,
            "blocking_artifact_count": (1 if status == "LIMITED_READER_CONTEXT" else 0),
            "important_artifact_count": 0,
            "manual_review_count": 0,
        },
        "checks": [],
        "missing_limited_artifact_impact": {},
        "manual_review_queue_groups": [],
    }


def _regenerate_quality(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    *,
    report_status: str,
    reader_status: str,
) -> tuple[
    ops_cli._FinalReaderBriefQualityResult,
    Path,
    Path,
    bytes,
    bytes,
]:
    reports_dir = tmp_path / "reports"
    json_path, html_path, json_bytes, html_bytes = _write_final_reader_brief(reports_dir)
    monkeypatch.setattr(
        ops_cli,
        "build_report_quality_gate_payload",
        lambda **_kwargs: _report_quality_payload(report_status),
    )
    monkeypatch.setattr(
        ops_cli,
        "build_reader_brief_quality_payload",
        lambda **_kwargs: _reader_quality_payload(reader_status),
    )
    result = ops_cli._regenerate_final_reader_brief_quality(
        as_of=AS_OF,
        reports_dir=reports_dir,
        project_root=tmp_path,
        reader_brief_json_path=json_path,
        reader_brief_html_path=html_path,
    )
    return result, json_path, html_path, json_bytes, html_bytes


def test_final_quality_evidence_binds_post_refresh_reader_brief_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    result, json_path, html_path, json_bytes, html_bytes = _regenerate_quality(
        monkeypatch,
        tmp_path,
        report_status="PASS",
        reader_status="OK",
    )

    assert result.blocker_code is None
    binding = result.reader_brief_bytes
    assert binding["binding_type"] == "final_reader_brief_bytes.v1"
    assert binding["reader_brief_json"] == {
        "path": str(json_path),
        "sha256": hashlib.sha256(json_bytes).hexdigest(),
        "size_bytes": len(json_bytes),
    }
    assert binding["reader_brief_html"] == {
        "path": str(html_path),
        "sha256": hashlib.sha256(html_bytes).hexdigest(),
        "size_bytes": len(html_bytes),
    }
    report_quality = json.loads(result.report_quality_paths[0].read_text(encoding="utf-8"))
    reader_quality = json.loads(result.reader_quality_paths[0].read_text(encoding="utf-8"))
    assert report_quality["final_reader_brief_bytes"] == binding
    assert reader_quality["final_reader_brief_bytes"] == binding


@pytest.mark.parametrize(
    ("report_status", "reader_status"),
    [
        ("PASS_WITH_WARNINGS", "LIMITED_READER_CONTEXT"),
        ("PASS", "PASS_WITH_WARNINGS"),
    ],
)
def test_final_quality_allows_explicit_limited_statuses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    report_status: str,
    reader_status: str,
) -> None:
    result, *_ = _regenerate_quality(
        monkeypatch,
        tmp_path,
        report_status=report_status,
        reader_status=reader_status,
    )

    assert result.blocker_code is None
    ops_cli._raise_for_final_quality_failure(
        result,
        evidence_path=tmp_path / "daily_ops_finalization.json",
    )


@pytest.mark.parametrize(
    ("report_status", "reader_status", "expected_code"),
    [
        ("FAIL", "OK", "DAILY_FINALIZATION_REPORT_QUALITY_FAILED"),
        ("PASS", "FAILED", "DAILY_FINALIZATION_READER_QUALITY_FAILED"),
    ],
)
def test_final_quality_failure_raises_typed_error_after_writing_evidence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    report_status: str,
    reader_status: str,
    expected_code: str,
) -> None:
    result, *_ = _regenerate_quality(
        monkeypatch,
        tmp_path,
        report_status=report_status,
        reader_status=reader_status,
    )
    evidence_path = tmp_path / "daily_ops_finalization.json"
    evidence_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._raise_for_final_quality_failure(
            result,
            evidence_path=evidence_path,
        )

    assert caught.value.code == expected_code
    assert caught.value.evidence_path == evidence_path
    assert result.report_quality_paths[0].exists()
    assert result.reader_quality_paths[0].exists()


@pytest.mark.parametrize(
    "reader_bytes",
    [
        (b'{"as_of":"2026-07-23","as_of":"2026-07-23",' b'"production_effect":"none"}'),
        (b'{"as_of":"2026-07-23","production_effect":"none",' b'"ambiguous_metric":NaN}'),
    ],
)
def test_final_reader_brief_strict_json_rejects_ambiguous_bytes(
    tmp_path: Path,
    reader_bytes: bytes,
) -> None:
    reports_dir = tmp_path / "reports"
    reports_dir.mkdir(parents=True)
    reader_json = ops_cli.default_reader_brief_json_path(reports_dir, AS_OF)
    reader_html = ops_cli.default_reader_brief_html_path(reports_dir, AS_OF)
    reader_json.write_bytes(reader_bytes)
    reader_html.write_text("<html>reader</html>", encoding="utf-8")

    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._regenerate_final_reader_brief_quality(
            as_of=AS_OF,
            reports_dir=reports_dir,
            project_root=tmp_path,
            reader_brief_json_path=reader_json,
            reader_brief_html_path=reader_html,
        )

    assert caught.value.code == "DAILY_FINALIZATION_READER_BRIEF_INVALID"


@pytest.mark.parametrize(
    "report_index_text",
    [
        (
            '{"schema_version":1,"report_type":"report_index",'
            '"as_of":"2026-07-23","as_of":"2026-07-23",'
            '"status":"PASS","production_effect":"none","reports":[]}'
        ),
        (
            '{"schema_version":1,"report_type":"report_index",'
            '"as_of":"2026-07-23","status":"PASS","production_effect":"none",'
            '"summary":{"report_count":Infinity},"reports":[]}'
        ),
    ],
)
def test_finalization_report_index_strict_json_fails_closed(
    tmp_path: Path,
    report_index_text: str,
) -> None:
    path = tmp_path / "report_index_2026-07-23.json"
    path.write_text(report_index_text, encoding="utf-8")

    payload, error = ops_cli._load_report_index_for_finalization(
        report_index_path=path,
        as_of=AS_OF,
    )

    assert payload["status"] == "FAILED"
    assert error is not None
    assert "cannot be loaded" in error


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("schema_version", 1.0),
        ("report_type", "wrong_report"),
        ("production_effect", "unknown"),
        ("status", "UNKNOWN"),
        ("reports", {}),
        ("summary", {"report_count": True}),
        ("summary", {"report_count": 1}),
    ],
)
def test_finalization_report_index_requires_exact_contract(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    _, _, _, _ = _write_final_reader_brief(tmp_path)
    path = ops_cli.default_report_index_json_path(tmp_path, AS_OF)
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload[field] = value
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded, error = ops_cli._load_report_index_for_finalization(
        report_index_path=path,
        as_of=AS_OF,
    )

    assert loaded["status"] == "FAILED"
    assert error is not None
    assert "contract invalid" in error


@pytest.mark.parametrize(
    ("field", "value", "error_code"),
    [
        ("schema_version", True, "DAILY_FINALIZATION_READER_BRIEF_CONTRACT_INVALID"),
        ("schema_version", 1.0, "DAILY_FINALIZATION_READER_BRIEF_CONTRACT_INVALID"),
        ("report_type", "wrong_report", "DAILY_FINALIZATION_READER_BRIEF_CONTRACT_INVALID"),
        ("status", "PASS", "DAILY_FINALIZATION_READER_BRIEF_STATUS_INVALID"),
        ("status", 1, "DAILY_FINALIZATION_READER_BRIEF_STATUS_INVALID"),
        ("status", "FAILED", "DAILY_FINALIZATION_READER_BRIEF_SOURCE_FAILED"),
    ],
)
def test_final_reader_brief_requires_exact_contract_and_status(
    tmp_path: Path,
    field: str,
    value: object,
    error_code: str,
) -> None:
    reader_json, reader_html, _, _ = _write_final_reader_brief(tmp_path)
    payload = json.loads(reader_json.read_text(encoding="utf-8"))
    payload[field] = value
    reader_json.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._regenerate_final_reader_brief_quality(
            as_of=AS_OF,
            reports_dir=tmp_path,
            project_root=tmp_path,
            reader_brief_json_path=reader_json,
            reader_brief_html_path=reader_html,
        )

    assert caught.value.code == error_code


def test_closed_market_finalization_rejects_stale_legacy_reader_artifact(
    tmp_path: Path,
) -> None:
    closed_as_of = date(2026, 7, 25)
    run_paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=closed_as_of,
            run_id="daily_ops_run:2026-07-25:closed",
            output_root=tmp_path / "runs",
            generated_at=datetime(2026, 7, 25, 20, 0, tzinfo=UTC),
        )
    )
    plan = build_daily_ops_plan(
        as_of=closed_as_of,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    run_report = DailyOpsRunReport(
        plan=plan,
        started_at=datetime(2026, 7, 25, 20, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 25, 20, 5, tzinfo=UTC),
        status="PASS_WITH_SKIPS",
        step_results=(),
    )
    evidence_path = run_paths.reports_dir / "daily_ops_finalization_2026-07-25.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_ops_canonical_finalization",
                "as_of": closed_as_of.isoformat(),
                "run_id": run_paths.run_id,
                "status": "PASS_WITH_WARNINGS",
                "source_daily_run_status": run_report.status,
                "report_quality_status": None,
                "reader_quality_status": None,
                "final_reader_brief_bytes": {},
                "quality_artifacts": {},
                "canonical_outputs": [],
                "legacy_outputs": [],
                "blocker": None,
                "safety_boundary": {
                    "production_effect": "none",
                    "writes_production_weights": False,
                    "writes_active_shadow_weights": False,
                    "broker_action_allowed": False,
                    "broker_action_taken": False,
                },
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    finalization = ops_cli.DailyOpsCanonicalFinalizationResult(
        status="PASS_WITH_WARNINGS",
        periodic_plan_path=run_paths.metadata_dir / "periodic_operations_plan_2026-07-25.json",
        daily_task_dashboard_path=run_paths.reports_dir / "daily_task_dashboard_2026-07-25.html",
        daily_task_dashboard_json_path=run_paths.reports_dir
        / "daily_task_dashboard_2026-07-25.json",
        daily_decision_summary_path=run_paths.reports_dir
        / "daily_decision_summary_2026-07-25.json",
        order_intent_candidates_path=run_paths.reports_dir
        / "order_intent_candidates_2026-07-25.json",
        reader_brief_final_paths=(),
        report_quality_paths=(),
        reader_quality_paths=(),
        finalization_evidence_path=evidence_path,
        canonical_outputs=(),
        legacy_outputs=(),
        report_quality_status=None,
        reader_quality_status=None,
    )
    legacy_reports_dir = tmp_path / "outputs" / "reports"
    legacy_reports_dir.mkdir(parents=True)
    stale_reader = legacy_reports_dir / "reader_brief_2026-07-25.json"
    stale_reader.write_text('{"status":"OK"}', encoding="utf-8")

    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._validate_daily_ops_finalization_closure(
            run_report=run_report,
            finalization=finalization,
            run_paths=run_paths,
            metadata_path=run_paths.metadata_dir / "daily_ops_run_metadata_2026-07-25.json",
            legacy_reports_dir=legacy_reports_dir,
            legacy_mode="mirror",
            expected_manifest_status="FINALIZING",
        )

    assert caught.value.code == "DAILY_FINALIZATION_NOT_DUE_READER_ARTIFACT_PRESENT"


def test_closure_failure_and_failed_downgrade_never_leave_pass_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_output_root = tmp_path / "runs"
    started_at = datetime(2026, 7, 24, 0, 0, tzinfo=UTC)
    finished_at = datetime(2026, 7, 24, 0, 5, tzinfo=UTC)

    def fake_controlled_runner(
        plan,
        *,
        project_root,
        env,
        run_id,
        diagnostics_dir,
        completion_callback,
        terminal_failure_callback,
    ) -> DailyOpsRunReport:
        del env, diagnostics_dir, terminal_failure_callback
        metadata = DailyOpsRunMetadata(
            schema_version=1,
            run_id=run_id,
            as_of=plan.as_of,
            generated_at=started_at,
            project_root=project_root,
            status="PASS",
            started_at=started_at,
            finished_at=finished_at,
            visibility_cutoff=finished_at,
            visibility_cutoff_source="test",
            input_visibility_status="PASS",
            input_visibility_issues=(),
            git={"commit": "test", "dirty": False},
            config_artifacts=(),
            rule_card_sha256=None,
            env_presence={},
            commands=(),
            step_results=(),
            pre_run_input_artifacts=(),
            produced_artifacts=(),
        )
        report = DailyOpsRunReport(
            plan=plan,
            started_at=started_at,
            finished_at=finished_at,
            status="PASS",
            step_results=(),
            metadata=metadata,
        )
        completion_callback(report)
        return report

    def fake_finalization(**kwargs) -> ops_cli.DailyOpsCanonicalFinalizationResult:
        run_paths = kwargs["run_paths"]
        as_of_text = kwargs["plan_date"].isoformat()
        return ops_cli.DailyOpsCanonicalFinalizationResult(
            status="PASS",
            periodic_plan_path=run_paths.metadata_dir
            / f"periodic_operations_plan_{as_of_text}.json",
            daily_task_dashboard_path=run_paths.reports_dir
            / f"daily_task_dashboard_{as_of_text}.html",
            daily_task_dashboard_json_path=run_paths.reports_dir
            / f"daily_task_dashboard_{as_of_text}.json",
            daily_decision_summary_path=run_paths.reports_dir
            / f"daily_decision_summary_{as_of_text}.json",
            order_intent_candidates_path=run_paths.reports_dir
            / f"order_intent_candidates_{as_of_text}.json",
            reader_brief_final_paths=(),
            report_quality_paths=(),
            reader_quality_paths=(),
            finalization_evidence_path=run_paths.reports_dir
            / f"daily_ops_finalization_{as_of_text}.json",
            canonical_outputs=(),
            legacy_outputs=(),
            report_quality_status=None,
            reader_quality_status=None,
        )

    def fail_closure(**kwargs) -> None:
        finalization = kwargs["finalization"]
        raise ops_cli.DailyOpsCanonicalFinalizationError(
            "INJECTED_CLOSURE_FAILURE",
            "injected after FINALIZING publication",
            evidence_path=finalization.finalization_evidence_path,
        )

    original_write_json_atomic = ops_cli.write_json_atomic
    manifest_statuses: list[str] = []

    def fail_failed_manifest_write(path: Path, payload, **kwargs):
        if path.name == "manifest.json":
            status = str(payload.get("status"))
            manifest_statuses.append(status)
            if status == "FAILED":
                raise OSError("injected FAILED manifest downgrade failure")
        return original_write_json_atomic(path, payload, **kwargs)

    monkeypatch.setattr(ops_cli, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(ops_cli, "run_daily_ops_plan", fake_controlled_runner)
    monkeypatch.setattr(ops_cli, "_finalize_daily_ops_canonical_outputs", fake_finalization)
    monkeypatch.setattr(ops_cli, "_validate_daily_ops_finalization_closure", fail_closure)
    monkeypatch.setattr(ops_cli, "write_json_atomic", fail_failed_manifest_write)

    result = CliRunner().invoke(
        app,
        [
            "ops",
            "daily-run",
            "--as-of",
            AS_OF.isoformat(),
            "--skip-risk-event-openai-precheck",
            "--run-output-root",
            str(run_output_root),
            "--run-id",
            "daily_ops_run:2026-07-23:manifest-failure",
        ],
    )

    manifest_path = next(run_output_root.rglob("manifest.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert result.exit_code == 1
    assert manifest_statuses == ["FINALIZING", "FAILED"]
    assert manifest["status"] == "FINALIZING"
    assert manifest["status"] != "PASS"


def test_finalization_closure_detects_reader_and_quality_contract_tampering(
    tmp_path: Path,
) -> None:
    run_paths = prepare_run_directories(
        build_run_artifact_paths(
            as_of=AS_OF,
            run_id="daily_ops_run:2026-07-23:closure",
            output_root=tmp_path / "runs",
            generated_at=datetime(2026, 7, 24, 0, 30, tzinfo=UTC),
        )
    )
    metadata_path = run_paths.metadata_dir / "daily_ops_run_metadata_2026-07-23.json"
    metadata_path.write_text('{"status":"PASS"}', encoding="utf-8")
    decision_path = run_paths.reports_dir / "daily_decision_summary_2026-07-23.json"
    decision_path.write_text(
        json.dumps(
            {
                "as_of": AS_OF.isoformat(),
                "production_effect": "none",
                "checksums": {
                    "daily_ops_metadata": hashlib.sha256(metadata_path.read_bytes()).hexdigest()
                },
            }
        ),
        encoding="utf-8",
    )
    periodic_plan_path = run_paths.metadata_dir / "periodic_operations_plan_2026-07-23.json"
    dashboard_path = run_paths.reports_dir / "daily_task_dashboard_2026-07-23.html"
    dashboard_json_path = run_paths.reports_dir / "daily_task_dashboard_2026-07-23.json"
    order_intent_path = run_paths.reports_dir / "order_intent_candidates_2026-07-23.json"
    for path, text in (
        (periodic_plan_path, '{"status":"PASS"}'),
        (dashboard_path, "<html>dashboard</html>"),
        (dashboard_json_path, '{"status":"PASS"}'),
        (order_intent_path, '{"production_effect":"none"}'),
    ):
        path.write_text(text, encoding="utf-8")
    reader_json, reader_html, reader_json_bytes, reader_html_bytes = _write_final_reader_brief(
        run_paths.reports_dir
    )
    binding = {
        "binding_type": "final_reader_brief_bytes.v1",
        "as_of": AS_OF.isoformat(),
        "reader_brief_json": {
            "path": str(reader_json),
            "sha256": hashlib.sha256(reader_json_bytes).hexdigest(),
            "size_bytes": len(reader_json_bytes),
        },
        "reader_brief_html": {
            "path": str(reader_html),
            "sha256": hashlib.sha256(reader_html_bytes).hexdigest(),
            "size_bytes": len(reader_html_bytes),
        },
    }
    report_quality_paths = (
        run_paths.reports_dir / "report_quality_gate_2026-07-23.json",
        run_paths.reports_dir / "report_quality_gate_2026-07-23.md",
    )
    reader_quality_paths = (
        run_paths.reports_dir / "reader_brief_quality_2026-07-23.json",
        run_paths.reports_dir / "reader_brief_quality_2026-07-23.md",
    )
    report_quality_paths[0].write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "report_quality_gate",
                "as_of": AS_OF.isoformat(),
                "status": "PASS",
                "report_quality_status": "PASS",
                "production_effect": "none",
                "final_reader_brief_bytes": binding,
            }
        ),
        encoding="utf-8",
    )
    report_quality_paths[1].write_text("# Report quality\n", encoding="utf-8")
    reader_quality_paths[0].write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "reader_brief_quality",
                "as_of": AS_OF.isoformat(),
                "status": "OK",
                "production_effect": "none",
                "final_reader_brief_bytes": binding,
            }
        ),
        encoding="utf-8",
    )
    reader_quality_paths[1].write_text("# Reader quality\n", encoding="utf-8")
    canonical_outputs = (
        metadata_path,
        periodic_plan_path,
        dashboard_path,
        dashboard_json_path,
        decision_path,
        order_intent_path,
        reader_json,
        reader_html,
        *report_quality_paths,
        *reader_quality_paths,
    )
    evidence_path = run_paths.reports_dir / "daily_ops_finalization_2026-07-23.json"
    evidence_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "report_type": "daily_ops_canonical_finalization",
                "as_of": AS_OF.isoformat(),
                "run_id": "daily_ops_run:2026-07-23:closure",
                "status": "PASS",
                "source_daily_run_status": "PASS",
                "report_quality_status": "PASS",
                "reader_quality_status": "OK",
                "final_reader_brief_bytes": binding,
                "quality_artifacts": {
                    "report_quality_json": ops_cli._artifact_byte_identity(report_quality_paths[0]),
                    "report_quality_markdown": ops_cli._artifact_byte_identity(
                        report_quality_paths[1]
                    ),
                    "reader_quality_json": ops_cli._artifact_byte_identity(reader_quality_paths[0]),
                    "reader_quality_markdown": ops_cli._artifact_byte_identity(
                        reader_quality_paths[1]
                    ),
                },
                "canonical_outputs": [str(path) for path in canonical_outputs],
                "legacy_outputs": [],
                "blocker": None,
                "safety_boundary": {
                    "production_effect": "none",
                    "writes_production_weights": False,
                    "writes_active_shadow_weights": False,
                    "broker_action_allowed": False,
                    "broker_action_taken": False,
                },
                "production_effect": "none",
            }
        ),
        encoding="utf-8",
    )
    plan = build_daily_ops_plan(
        as_of=AS_OF,
        project_root=tmp_path,
        skip_risk_event_openai_precheck=True,
    )
    run_report = DailyOpsRunReport(
        plan=plan,
        started_at=datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
        finished_at=datetime(2026, 7, 24, 0, 20, tzinfo=UTC),
        status="PASS",
        step_results=(),
    )
    finalization = ops_cli.DailyOpsCanonicalFinalizationResult(
        status="PASS",
        periodic_plan_path=periodic_plan_path,
        daily_task_dashboard_path=dashboard_path,
        daily_task_dashboard_json_path=dashboard_json_path,
        daily_decision_summary_path=decision_path,
        order_intent_candidates_path=order_intent_path,
        reader_brief_final_paths=(reader_html, reader_json),
        report_quality_paths=report_quality_paths,
        reader_quality_paths=reader_quality_paths,
        finalization_evidence_path=evidence_path,
        canonical_outputs=canonical_outputs,
        legacy_outputs=(),
        report_quality_status="PASS",
        reader_quality_status="OK",
    )
    write_run_manifest(
        paths=run_paths,
        project_root=tmp_path,
        status="FINALIZING",
        visibility_cutoff=run_report.finished_at,
        visibility_cutoff_source="test",
        legacy_output_mode="off",
        input_artifacts=(),
        canonical_output_artifacts=(
            *canonical_outputs,
            evidence_path,
        ),
        legacy_output_artifacts=(),
    )

    ops_cli._validate_daily_ops_finalization_closure(
        run_report=run_report,
        finalization=finalization,
        run_paths=run_paths,
        metadata_path=metadata_path,
        legacy_reports_dir=tmp_path / "outputs" / "reports",
        legacy_mode="off",
        expected_manifest_status="FINALIZING",
    )

    reader_json.write_bytes(reader_json.read_bytes() + b"\n")
    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._validate_daily_ops_finalization_closure(
            run_report=run_report,
            finalization=finalization,
            run_paths=run_paths,
            metadata_path=metadata_path,
            legacy_reports_dir=tmp_path / "outputs" / "reports",
            legacy_mode="off",
            expected_manifest_status="FINALIZING",
        )

    assert caught.value.code == "DAILY_FINALIZATION_READER_BYTES_DRIFT"

    reader_json.write_bytes(reader_json_bytes)
    for quality_path in (report_quality_paths[0], reader_quality_paths[0]):
        original_bytes = quality_path.read_bytes()
        for schema_version in (True, 1.0, "1"):
            payload = json.loads(original_bytes)
            payload["schema_version"] = schema_version
            quality_path.write_text(json.dumps(payload), encoding="utf-8")
            with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
                ops_cli._validate_daily_ops_finalization_closure(
                    run_report=run_report,
                    finalization=finalization,
                    run_paths=run_paths,
                    metadata_path=metadata_path,
                    legacy_reports_dir=tmp_path / "outputs" / "reports",
                    legacy_mode="off",
                    expected_manifest_status="FINALIZING",
                )
            assert caught.value.code == "DAILY_FINALIZATION_QUALITY_CONTRACT_INVALID"
        invalid_code = (
            "DAILY_FINALIZATION_REPORT_QUALITY_INVALID"
            if quality_path == report_quality_paths[0]
            else "DAILY_FINALIZATION_READER_QUALITY_INVALID"
        )
        for invalid_text in (
            '{"schema_version":1,"schema_version":1}',
            '{"schema_version":1,"value":NaN}',
        ):
            quality_path.write_text(invalid_text, encoding="utf-8")
            with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
                ops_cli._validate_daily_ops_finalization_closure(
                    run_report=run_report,
                    finalization=finalization,
                    run_paths=run_paths,
                    metadata_path=metadata_path,
                    legacy_reports_dir=tmp_path / "outputs" / "reports",
                    legacy_mode="off",
                    expected_manifest_status="FINALIZING",
                )
            assert caught.value.code == invalid_code
        quality_path.write_bytes(original_bytes)

    evidence_bytes = evidence_path.read_bytes()
    evidence_payload = json.loads(evidence_bytes)
    evidence_payload["schema_version"] = True
    evidence_path.write_text(json.dumps(evidence_payload), encoding="utf-8")
    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._validate_daily_ops_finalization_closure(
            run_report=run_report,
            finalization=finalization,
            run_paths=run_paths,
            metadata_path=metadata_path,
            legacy_reports_dir=tmp_path / "outputs" / "reports",
            legacy_mode="off",
            expected_manifest_status="FINALIZING",
        )
    assert caught.value.code == "DAILY_FINALIZATION_EVIDENCE_CONTRACT_INVALID"
    evidence_path.write_bytes(evidence_bytes)

    for field, value, error_code in (
        (
            "run_id",
            "daily_ops_run:wrong",
            "DAILY_FINALIZATION_EVIDENCE_RUN_CONTEXT_INVALID",
        ),
        (
            "source_daily_run_status",
            "FAILED",
            "DAILY_FINALIZATION_EVIDENCE_RUN_CONTEXT_INVALID",
        ),
    ):
        evidence_payload = json.loads(evidence_bytes)
        evidence_payload[field] = value
        evidence_path.write_text(json.dumps(evidence_payload), encoding="utf-8")
        with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
            ops_cli._validate_daily_ops_finalization_closure(
                run_report=run_report,
                finalization=finalization,
                run_paths=run_paths,
                metadata_path=metadata_path,
                legacy_reports_dir=tmp_path / "outputs" / "reports",
                legacy_mode="off",
                expected_manifest_status="FINALIZING",
            )
        assert caught.value.code == error_code
    evidence_path.write_bytes(evidence_bytes)

    evidence_payload = json.loads(evidence_bytes)
    evidence_payload["final_reader_brief_bytes"]["binding_type"] = "wrong_binding"
    evidence_path.write_text(json.dumps(evidence_payload), encoding="utf-8")
    with pytest.raises(ops_cli.DailyOpsCanonicalFinalizationError) as caught:
        ops_cli._validate_daily_ops_finalization_closure(
            run_report=run_report,
            finalization=finalization,
            run_paths=run_paths,
            metadata_path=metadata_path,
            legacy_reports_dir=tmp_path / "outputs" / "reports",
            legacy_mode="off",
            expected_manifest_status="FINALIZING",
        )
    assert caught.value.code == "DAILY_FINALIZATION_READER_BINDING_CONTRACT_INVALID"
