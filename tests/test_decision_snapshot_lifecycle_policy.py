from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.decision_snapshot_lifecycle_policy import (
    PASS_STATUS,
    PASS_WITH_WARNINGS_STATUS,
    SNAPSHOT_AVAILABLE,
    SNAPSHOT_MISSING_BLOCKING,
    SNAPSHOT_MISSING_NON_BLOCKING,
    SNAPSHOT_NOT_DUE,
    build_decision_snapshot_lifecycle_policy_payload,
    validate_decision_snapshot_lifecycle_policy_payload,
)

RUN_DATE = date(2026, 5, 4)


def test_snapshot_lifecycle_available_when_canonical_snapshot_exists(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "decision_snapshots"
    snapshot_path = _write_decision_snapshot(snapshot_dir, RUN_DATE)

    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=RUN_DATE,
        snapshot_dir=snapshot_dir,
        report_index_payload=_report_index_payload(tmp_path),
        today=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_decision_snapshot_lifecycle_policy_payload(payload)

    assert payload["snapshot_lifecycle_status"] == SNAPSHOT_AVAILABLE
    assert payload["summary"]["snapshot_path"] == str(snapshot_path)
    assert payload["summary"]["snapshot_signal_date"] == RUN_DATE.isoformat()
    assert validation["validation_status"] == PASS_STATUS


def test_snapshot_lifecycle_missing_blocks_due_trading_day(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "decision_snapshots"
    target_path = snapshot_dir / f"decision_snapshot_{RUN_DATE.isoformat()}.json"

    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=RUN_DATE,
        snapshot_dir=snapshot_dir,
        report_index_payload=_report_index_payload(tmp_path),
        today=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_decision_snapshot_lifecycle_policy_payload(payload)

    assert payload["snapshot_lifecycle_status"] == SNAPSHOT_MISSING_BLOCKING
    assert payload["summary"]["blocking_impact"] == (
        "blocks_same_day_reader_brief_and_decision_conclusion"
    )
    assert "canonical_decision_snapshot_missing" in payload["snapshot_check"][
        "invalid_reasons"
    ][0]
    assert not target_path.exists()
    assert validation["validation_status"] == PASS_WITH_WARNINGS_STATUS


def test_snapshot_lifecycle_not_due_for_closed_market_date(tmp_path: Path) -> None:
    closed_date = date(2026, 5, 2)

    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=closed_date,
        snapshot_dir=tmp_path / "decision_snapshots",
        report_index_payload=_report_index_payload(tmp_path),
        today=closed_date,
        project_root=tmp_path,
    )

    assert payload["snapshot_lifecycle_status"] == SNAPSHOT_NOT_DUE
    assert payload["summary"]["market_session_status"] == "CLOSED_MARKET"
    assert "market_session_not_trading_day" in payload["snapshot_check"]["invalid_reasons"]


def test_snapshot_lifecycle_latest_context_is_non_blocking_but_limited(
    tmp_path: Path,
) -> None:
    snapshot_dir = tmp_path / "decision_snapshots"
    _write_decision_snapshot(snapshot_dir, date(2026, 5, 1))

    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=RUN_DATE,
        snapshot_dir=snapshot_dir,
        report_index_payload=_report_index_payload(tmp_path),
        allow_latest_context=True,
        today=RUN_DATE,
        project_root=tmp_path,
    )

    assert payload["snapshot_lifecycle_status"] == SNAPSHOT_MISSING_NON_BLOCKING
    assert payload["summary"]["latest_available_snapshot_date"] == "2026-05-01"
    assert payload["snapshot_check"]["missing_snapshot_behavior"] == (
        "do_not_fabricate_target_snapshot_use_latest_context_only"
    )


def test_snapshot_lifecycle_cli_writes_report_and_validation(tmp_path: Path) -> None:
    reports_dir = tmp_path / "outputs" / "reports"
    report_index = _report_index_payload(tmp_path)
    report_index_path = reports_dir / "report_index_2026-05-04.json"
    report_index_path.parent.mkdir(parents=True, exist_ok=True)
    report_index_path.write_text(json.dumps(report_index, ensure_ascii=False), encoding="utf-8")
    snapshot_dir = tmp_path / "decision_snapshots"
    target_path = snapshot_dir / "decision_snapshot_2026-05-04.json"
    runner = CliRunner()

    report_result = runner.invoke(
        app,
        [
            "reports",
            "decision-snapshot-lifecycle-policy",
            "--as-of",
            RUN_DATE.isoformat(),
            "--reports-dir",
            str(reports_dir),
            "--snapshot-dir",
            str(snapshot_dir),
            "--report-index-path",
            str(report_index_path),
            "--project-root",
            str(tmp_path),
            "--today",
            RUN_DATE.isoformat(),
        ],
    )
    assert report_result.exit_code == 0, report_result.output

    validation_result = runner.invoke(
        app,
        [
            "reports",
            "validate-decision-snapshot-lifecycle-policy",
            "--latest",
            "--reports-dir",
            str(reports_dir),
        ],
    )
    assert validation_result.exit_code == 0, validation_result.output

    report_payload = json.loads(
        (reports_dir / "decision_snapshot_lifecycle_policy_2026-05-04.json").read_text(
            encoding="utf-8"
        )
    )
    validation_payload = json.loads(
        (
            reports_dir / "decision_snapshot_lifecycle_policy_validation_2026-05-04.json"
        ).read_text(encoding="utf-8")
    )
    assert report_payload["snapshot_lifecycle_status"] == SNAPSHOT_MISSING_BLOCKING
    assert validation_payload["validation_status"] == PASS_WITH_WARNINGS_STATUS
    assert validation_payload["input_artifacts"]["decision_snapshot_lifecycle_policy"].endswith(
        "decision_snapshot_lifecycle_policy_2026-05-04.json"
    )
    assert not target_path.exists()


def test_reader_brief_decision_snapshot_lifecycle_summary(tmp_path: Path) -> None:
    payload = build_decision_snapshot_lifecycle_policy_payload(
        as_of=RUN_DATE,
        snapshot_dir=tmp_path / "decision_snapshots",
        report_index_payload=_report_index_payload(tmp_path),
        today=RUN_DATE,
        project_root=tmp_path,
    )
    validation = validate_decision_snapshot_lifecycle_policy_payload(payload)
    report_path = tmp_path / "decision_snapshot_lifecycle_policy_2026-05-04.json"
    validation_path = (
        tmp_path / "decision_snapshot_lifecycle_policy_validation_2026-05-04.json"
    )
    report_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    validation_path.write_text(json.dumps(validation, ensure_ascii=False), encoding="utf-8")

    summary = reader_brief._decision_snapshot_lifecycle_policy_summary(
        {
            "reports": [
                {
                    "report_id": "decision_snapshot_lifecycle_policy",
                    "latest_artifact_path": str(report_path),
                },
                {
                    "report_id": "decision_snapshot_lifecycle_policy_validation",
                    "latest_artifact_path": str(validation_path),
                },
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["snapshot_lifecycle_status"] == SNAPSHOT_MISSING_BLOCKING
    assert summary["validation_status"] == PASS_WITH_WARNINGS_STATUS
    assert summary["snapshot_exists"] is False
    assert summary["production_effect"] == "none"


def _write_decision_snapshot(snapshot_dir: Path, as_of: date) -> Path:
    path = snapshot_dir / f"decision_snapshot_{as_of.isoformat()}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "snapshot_id": f"decision_snapshot:{as_of.isoformat()}",
                "signal_date": as_of.isoformat(),
                "generated_at": "2026-05-04T20:30:00+00:00",
                "scores": {},
                "positions": {},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return path


def _report_index_payload(tmp_path: Path) -> dict[str, object]:
    reports: list[dict[str, object]] = []
    for report_id in (
        "daily_decision_summary",
        "daily_report",
        "calculation_explainers",
        "reader_brief",
        "report_index",
    ):
        source_path = tmp_path / "sources" / f"{report_id}.json"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            json.dumps({"report_type": report_id, "status": "PASS"}, ensure_ascii=False),
            encoding="utf-8",
        )
        reports.append(
            {
                "report_id": report_id,
                "latest_artifact_path": str(source_path),
                "artifact_status": "AVAILABLE",
                "freshness_status": "FRESH",
                "production_effect": "none",
            }
        )
    return {
        "schema_version": 1,
        "report_type": "report_index",
        "as_of": RUN_DATE.isoformat(),
        "reports": reports,
        "production_effect": "none",
    }
