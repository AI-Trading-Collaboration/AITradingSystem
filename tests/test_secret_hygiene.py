from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.secret_hygiene import render_secret_scan_report, scan_secrets


def test_secret_scan_redacts_suspected_literal(tmp_path: Path) -> None:
    source = tmp_path / "config.yaml"
    secret_value = "sk_test_123456789012345678901234"
    source.write_text(f"api_key: {secret_value}\n", encoding="utf-8")

    report = scan_secrets(paths=(source,), as_of=date(2026, 5, 4))
    markdown = render_secret_scan_report(report)

    assert report.status == "FAIL"
    assert report.error_count == 1
    assert "sk_t...1234" in markdown
    assert secret_value not in markdown
    assert "环境变量或安全密钥管理" in markdown


def test_security_scan_secrets_cli_writes_report(tmp_path: Path) -> None:
    clean_dir = tmp_path / "docs"
    clean_dir.mkdir()
    (clean_dir / "note.md").write_text("API key 从环境变量读取。\n", encoding="utf-8")
    output_path = tmp_path / "secret_hygiene.md"

    result = CliRunner().invoke(
        app,
        [
            "security",
            "scan-secrets",
            "--as-of",
            "2026-05-04",
            "--scan-paths",
            str(clean_dir),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "Secret hygiene：PASS" in result.output
    assert output_path.exists()
    assert "未发现疑似 secret" in output_path.read_text(encoding="utf-8")
