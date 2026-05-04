from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import load_risk_events
from ai_trading_system.risk_event_sources import (
    import_risk_event_occurrences_csv,
    render_risk_event_occurrence_import_report,
    write_risk_event_occurrences_yaml,
)
from ai_trading_system.risk_events import (
    load_risk_event_occurrence_store,
    validate_risk_event_occurrence_store,
)


def test_import_risk_event_occurrences_csv_merges_multiple_evidence_rows(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    _write_csv(
        input_path,
        [
            _row(source_name="bis_release", source_type="primary_source"),
            _row(
                source_name="manual_policy_review",
                source_type="manual_input",
                source_url="",
                evidence_notes="人工复核确认政策事件与 event_id 匹配。",
            ),
        ],
    )

    report = import_risk_event_occurrences_csv(input_path)
    markdown = render_risk_event_occurrence_import_report(report)

    assert report.status == "PASS"
    assert report.row_count == 2
    assert report.occurrence_count == 1
    assert len(report.checksum_sha256) == 64
    assert len(report.occurrences[0].evidence_sources) == 2
    assert report.occurrences[0].evidence_grade == "A"
    assert report.occurrences[0].severity == "high"
    assert report.occurrences[0].probability == "confirmed"
    assert report.occurrences[0].scope == "ai_bucket"
    assert report.occurrences[0].action_class == "position_gate_eligible"
    assert report.occurrences[0].summary == "人工复核后的 AI 芯片出口限制升级记录。"
    assert "- CSV 行数：2" in markdown
    assert "导入发生记录数：1" in markdown


def test_import_risk_event_occurrences_csv_rejects_inconsistent_duplicate_rows(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    _write_csv(
        input_path,
        [
            _row(source_name="bis_release", source_type="primary_source"),
            _row(
                status="resolved",
                source_name="manual_policy_review",
                source_type="manual_input",
            ),
        ],
    )

    report = import_risk_event_occurrences_csv(input_path)

    assert report.passed is False
    assert report.occurrences == ()
    assert "inconsistent_duplicate_occurrence" in {issue.code for issue in report.issues}


def test_import_risk_event_occurrences_csv_rejects_invalid_dates_status_and_source_type(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    _write_csv(
        input_path,
        [
            _row(
                status="pending",
                source_type="blog",
                triggered_at="2026/05/01",
                captured_at="not-a-date",
            )
        ],
    )

    report = import_risk_event_occurrences_csv(input_path)
    codes = {issue.code for issue in report.issues}

    assert report.passed is False
    assert report.occurrences == ()
    assert "invalid_risk_event_occurrence_status" in codes
    assert "invalid_risk_event_source_type" in codes
    assert "invalid_csv_date" in codes


def test_import_risk_event_occurrences_csv_warns_on_missing_url_for_non_manual_source(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    _write_csv(
        input_path,
        [_row(source_name="vendor_terminal", source_type="paid_vendor", source_url="")],
    )

    report = import_risk_event_occurrences_csv(input_path)

    assert report.status == "PASS_WITH_WARNINGS"
    assert report.occurrence_count == 1
    assert "missing_risk_event_evidence_url" in {issue.code for issue in report.issues}


def test_write_imported_occurrences_yaml_round_trips_through_existing_validation(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    output_dir = tmp_path / "occurrences"
    _write_csv(
        input_path,
        [
            _row(source_name="bis_release", source_type="primary_source"),
            _row(
                source_name="manual_policy_review",
                source_type="manual_input",
                source_url="",
            ),
        ],
    )

    import_report = import_risk_event_occurrences_csv(input_path)
    written_paths = write_risk_event_occurrences_yaml(import_report, output_dir)
    store = load_risk_event_occurrence_store(output_dir)
    validation_report = validate_risk_event_occurrence_store(
        store=store,
        risk_events=load_risk_events(),
        as_of=date(2026, 5, 2),
    )

    assert len(written_paths) == 1
    assert written_paths[0].name == "ai_chip_export_control_upgrade_2026_05_01.yaml"
    assert store.loaded[0].occurrence.occurrence_id == (
        "ai_chip_export_control_upgrade_2026_05_01"
    )
    assert len(store.loaded[0].occurrence.evidence_sources) == 2
    assert validation_report.status == "PASS"


def test_write_imported_occurrences_yaml_refuses_failed_import(tmp_path: Path) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    _write_csv(input_path, [_row(status="pending")])
    report = import_risk_event_occurrences_csv(input_path)

    with pytest.raises(ValueError, match="CSV 导入存在错误"):
        write_risk_event_occurrences_yaml(report, tmp_path / "occurrences")


def test_risk_events_import_occurrences_csv_cli_writes_yaml_and_reports(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "reviewed_occurrences.csv"
    output_dir = tmp_path / "occurrences"
    import_report_path = tmp_path / "risk_event_import.md"
    validation_report_path = tmp_path / "risk_event_occurrences.md"
    _write_csv(input_path, [_row(source_name="bis_release", source_type="primary_source")])

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "import-occurrences-csv",
            "--input-path",
            str(input_path),
            "--output-dir",
            str(output_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(import_report_path),
            "--validation-report-path",
            str(validation_report_path),
        ],
    )

    assert result.exit_code == 0
    assert "风险事件发生记录 CSV 导入状态：PASS" in result.output
    assert (output_dir / "ai_chip_export_control_upgrade_2026_05_01.yaml").exists()
    assert import_report_path.exists()
    assert validation_report_path.exists()


def _write_csv(input_path: Path, rows: list[dict[str, str]]) -> None:
    columns = [
        "occurrence_id",
        "event_id",
        "status",
        "triggered_at",
        "last_confirmed_at",
        "source_name",
        "source_type",
        "captured_at",
        "summary",
        "resolved_at",
        "source_url",
        "published_at",
        "evidence_notes",
        "notes",
        "evidence_grade",
        "severity",
        "probability",
        "scope",
        "time_sensitivity",
        "reversibility",
        "action_class",
    ]
    lines = [",".join(columns)]
    for row in rows:
        lines.append(",".join(_csv_cell(row.get(column, "")) for column in columns))
    input_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _row(**overrides: str) -> dict[str, str]:
    values = {
        "occurrence_id": "ai_chip_export_control_upgrade_2026_05_01",
        "event_id": "ai_chip_export_control_upgrade",
        "status": "active",
        "triggered_at": "2026-05-01",
        "last_confirmed_at": "2026-05-02",
        "source_name": "bis_release",
        "source_type": "primary_source",
        "captured_at": "2026-05-02",
        "summary": "人工复核后的 AI 芯片出口限制升级记录。",
        "resolved_at": "",
        "source_url": "https://example.test/policy-release",
        "published_at": "2026-05-01",
        "evidence_notes": "证据来源已由人工复核。",
        "notes": "CSV import unit test.",
        "evidence_grade": "A",
        "severity": "high",
        "probability": "confirmed",
        "scope": "ai_bucket",
        "time_sensitivity": "high",
        "reversibility": "partly_reversible",
        "action_class": "position_gate_eligible",
    }
    values.update(overrides)
    return values


def _csv_cell(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'
