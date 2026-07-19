from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.etf_portfolio import dynamic_v3_drawdown_casebook as casebook


def test_drawdown_event_casebook_builds_and_validates(tmp_path: Path) -> None:
    result = casebook.build_drawdown_event_casebook(
        output_dir=tmp_path / "drawdown_event_casebook",
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    validation = casebook.validate_drawdown_event_casebook_artifact(
        casebook_run_id=result["casebook_run_id"],
        output_dir=tmp_path / "drawdown_event_casebook",
    )
    payload = casebook.drawdown_event_casebook_report_payload(
        casebook_run_id=result["casebook_run_id"],
        output_dir=tmp_path / "drawdown_event_casebook",
    )
    data = result["drawdown_event_casebook"]

    assert validation["status"] == "PASS"
    assert payload["drawdown_event_casebook"]["casebook_run_id"] == result["casebook_run_id"]
    assert data["event_count"] >= 5
    assert data["worst_event"] == "semiconductor_pullback_2024_07"
    assert "tech_drawdown" in data["regime_coverage"]
    assert data["next_review_action"] == "manual_qualitative_review_only"
    assert data["evidence_role"] == "MANUAL_DIAGNOSTIC"
    assert data["quantitative_evidence_eligible"] is False
    assert data["promotion_evidence_eligible"] is False
    assert payload["input_snapshot"]["schema_version"].endswith(".v2")
    assert data["not_trading_signal"] is True
    assert data["data_downloaded_by_casebook"] is False
    assert data["pipelines_executed_by_casebook"] is False
    assert "drawdown_casebook_event_count" in result["drawdown_event_casebook_reader_brief"]


def test_drawdown_event_casebook_validation_fails_incomplete_event(tmp_path: Path) -> None:
    config = yaml.safe_load(
        casebook.DEFAULT_DRAWDOWN_EVENT_CASEBOOK_CONFIG_PATH.read_text(encoding="utf-8")
    )
    config["events"][0].pop("review_notes")
    config_path = tmp_path / "drawdown_event_casebook_v1.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(ValueError, match="manual events are incomplete"):
        casebook.build_drawdown_event_casebook(
            config_path=config_path,
            output_dir=tmp_path / "drawdown_event_casebook",
            generated_at=datetime(2026, 6, 15, tzinfo=UTC),
        )


def test_drawdown_event_casebook_snapshot_and_view_tamper_fail_closed(tmp_path: Path) -> None:
    output_dir = tmp_path / "drawdown_event_casebook"
    result = casebook.build_drawdown_event_casebook(
        output_dir=output_dir,
        generated_at=datetime(2026, 6, 15, tzinfo=UTC),
    )
    root = Path(result["casebook_dir"])
    report = root / "drawdown_event_casebook_report.md"
    report.write_text(report.read_text(encoding="utf-8") + "tampered\n", encoding="utf-8")

    validation = casebook.validate_drawdown_event_casebook_artifact(
        casebook_run_id=result["casebook_run_id"], output_dir=output_dir
    )

    assert validation["status"] == "FAIL"
    assert any(
        row["check_id"] == "snapshot_view_hashes" and row["passed"] is False
        for row in validation["checks"]
    )


def test_drawdown_event_casebook_cli_report_and_validate(tmp_path: Path) -> None:
    output_dir = tmp_path / "drawdown_event_casebook"
    result = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "drawdown-event-casebook",
            "report",
            "--output-dir",
            str(output_dir),
        ],
    )
    assert result.exit_code == 0
    assert "event_count=5" in result.output
    assert "not_trading_signal=true" in result.output
    casebook_run_id = next(
        line.split("=", 1)[1]
        for line in result.output.splitlines()
        if line.startswith("casebook_run_id=")
    )

    validation = CliRunner().invoke(
        app,
        [
            "etf",
            "dynamic-v3-rescue",
            "validate-drawdown-event-casebook",
            "--casebook-run-id",
            casebook_run_id,
            "--output-dir",
            str(output_dir),
        ],
    )
    assert validation.exit_code == 0
    assert "status=PASS" in validation.output
