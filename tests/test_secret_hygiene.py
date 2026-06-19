from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.secret_hygiene import render_secret_scan_report, scan_secrets


def test_secret_scan_redacts_suspected_literal(tmp_path: Path) -> None:
    source = tmp_path / "config.yaml"
    secret_value = "DUMMY_SECRET_VALUE_NOT_A_REAL_KEY"
    source.write_text(f"api_key: {secret_value}\n", encoding="utf-8")

    report = scan_secrets(paths=(source,), as_of=date(2026, 5, 4))
    markdown = render_secret_scan_report(report)

    assert report.status == "FAIL"
    assert report.error_count == 1
    assert "DUMM..._KEY" in markdown
    assert secret_value not in markdown
    assert "环境变量或安全密钥管理" in markdown


def test_secret_scan_detects_secret_key_literal(tmp_path: Path) -> None:
    source = tmp_path / "config.yaml"
    secret_value = "TEST_ONLY_NON_PROVIDER_SECRET"
    source.write_text(f"secret_key: {secret_value}\n", encoding="utf-8")

    report = scan_secrets(paths=(source,), as_of=date(2026, 6, 19))

    assert report.status == "FAIL"
    assert report.error_count == 1
    assert report.findings[0].matched_label == "secret_key"


def test_secret_scan_allows_generated_report_source_location_metadata(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "outputs" / "reports"
    report_dir.mkdir(parents=True)
    (report_dir / "executable_binding_safety_audit_2026-06-17.json").write_text(
        "\n".join(
            [
                '"finding_id": "api_key:executable_research_binding.py:2796",',
                '"warnings": "account_id:executable_research_binding.py:2795,'
                'api_key:executable_research_binding.py:2796",',
                '"api_key:executable_research_binding.py:3043",',
            ]
        ),
        encoding="utf-8",
    )
    (report_dir / "executable_binding_safety_audit_2026-06-17.md").write_text(
        "|api_key:executable_research_binding.py:2796|"
        '"api_key": ("api_key", "api key", "apikey", "secret_key")|2796|\n',
        encoding="utf-8",
    )
    (report_dir / "executable_binding_safety_audit_2026-06-18.md").write_text(
        "- warnings: account_id:executable_research_binding.py:2795,"
        "api_key:executable_research_binding.py:2796\n",
        encoding="utf-8",
    )
    (
        report_dir / "next_candidate_research_cycle_snapshot_2026-06-17.json"
    ).write_text(
        '"warnings": "api_key:executable_research_binding.py:2971"',
        encoding="utf-8",
    )
    (report_dir / "reader_brief_consistency_pack_2026-06-19.json").write_text(
        '"decision_state": "# Report\\n- warnings: '
        'api_key:executable_research_binding.py:3043\\n"',
        encoding="utf-8",
    )

    report = scan_secrets(paths=(report_dir,), as_of=date(2026, 6, 18))

    assert report.status == "PASS"
    assert report.error_count == 0


def test_generated_report_allowlist_still_flags_real_secret_literal(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "outputs" / "reports"
    report_dir.mkdir(parents=True)
    secret_value = "TEST_ONLY_NON_PROVIDER_SECRET"
    source = report_dir / "executable_binding_safety_audit_2026-06-17.json"
    source.write_text(
        "\n".join(
            [
                '"finding_id": "api_key:executable_research_binding.py:2796",',
                f'"api_key": "{secret_value}",',
                '"warnings": "api_key:executable_research_binding.py:3043,'
                f'api_key:{secret_value}",',
            ]
        ),
        encoding="utf-8",
    )

    report = scan_secrets(paths=(report_dir,), as_of=date(2026, 6, 18))
    markdown = render_secret_scan_report(report)

    assert report.status == "FAIL"
    assert report.error_count == 2
    assert secret_value not in markdown


def test_reader_brief_metadata_allowlist_still_flags_real_secret_literal(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "outputs" / "reports"
    report_dir.mkdir(parents=True)
    secret_value = "DUMMY_SECRET_VALUE_NOT_A_REAL_KEY"
    source = report_dir / "reader_brief_consistency_pack_2026-06-19.json"
    source.write_text(
        '"decision_state": "# Report\\n- warnings: '
        f'api_key:executable_research_binding.py:3043,api_key:{secret_value}\\n"',
        encoding="utf-8",
    )

    report = scan_secrets(paths=(source,), as_of=date(2026, 6, 19))
    markdown = render_secret_scan_report(report)

    assert report.status == "FAIL"
    assert report.error_count == 1
    assert secret_value not in markdown


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
