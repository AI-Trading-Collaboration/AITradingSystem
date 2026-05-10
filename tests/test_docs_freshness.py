from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.docs_freshness import validate_docs_freshness


def test_docs_freshness_passes_when_last_updated_covers_status_dates(
    tmp_path: Path,
) -> None:
    doc = tmp_path / "ok.md"
    doc.write_text(
        "# OK\n\n最后更新：2026-05-10\n\n- 2026-05-09：READY\n",
        encoding="utf-8",
    )

    report = validate_docs_freshness((doc,))

    assert report.passed is True
    assert report.records[0].last_updated.isoformat() == "2026-05-10"
    assert report.records[0].latest_status_date.isoformat() == "2026-05-09"


def test_docs_freshness_fails_when_last_updated_missing(tmp_path: Path) -> None:
    doc = tmp_path / "missing.md"
    doc.write_text("# Missing\n\n- 2026-05-09：READY\n", encoding="utf-8")

    report = validate_docs_freshness((doc,))

    assert report.passed is False
    assert report.issues[0].code == "missing_last_updated"


def test_docs_freshness_fails_when_status_date_is_newer_than_last_updated(
    tmp_path: Path,
) -> None:
    doc = tmp_path / "stale.md"
    doc.write_text(
        "# Stale\n\n最后更新：2026-05-09\n\n- 2026-05-10：DONE\n",
        encoding="utf-8",
    )

    report = validate_docs_freshness((doc,))

    assert report.passed is False
    assert report.issues[0].code == "stale_last_updated"


def test_docs_validate_freshness_cli_writes_report_and_fails_on_issue(
    tmp_path: Path,
) -> None:
    doc = tmp_path / "stale.md"
    output_path = tmp_path / "freshness.md"
    doc.write_text(
        "# Stale\n\n最后更新：2026-05-09\n\n- 2026-05-10：DONE\n",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "docs",
            "validate-freshness",
            "--path",
            str(doc),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 1
    assert "文档新鲜度：FAIL" in result.output
    assert output_path.exists()
    assert "stale_last_updated" in output_path.read_text(encoding="utf-8")
