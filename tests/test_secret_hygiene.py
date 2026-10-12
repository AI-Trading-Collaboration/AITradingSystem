from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from typer.testing import CliRunner

from ai_trading_system import secret_hygiene
from ai_trading_system.cli import app
from ai_trading_system.secret_hygiene import (
    SecretScanMode,
    render_secret_scan_report,
    scan_secrets,
)

SCAN_TIME = datetime(2026, 10, 12, 1, 0, tzinfo=UTC)


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
    (report_dir / "next_candidate_research_cycle_snapshot_2026-06-17.json").write_text(
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


def _write_tree(root: Path, file_count: int = 3) -> list[Path]:
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for index in range(file_count):
        path = root / f"report_{index}.md"
        path.write_text(f"# report {index}\nAPI key 从环境变量读取。\n", encoding="utf-8")
        paths.append(path)
    return paths


def _rewrite_keeping_size_and_mtime(path: Path, line: str) -> None:
    original = path.stat()
    content = line.encode("utf-8")
    assert len(content) + 1 <= original.st_size
    path.write_bytes(content + b" " * (original.st_size - len(content) - 1) + b"\n")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    assert (path.stat().st_size, path.stat().st_mtime_ns) == (
        original.st_size,
        original.st_mtime_ns,
    )


def test_incremental_scan_reuses_unchanged_clean_files(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    _write_tree(root)
    cache_path = tmp_path / "cache" / "scan_cache.json"

    first = scan_secrets(
        paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME
    )
    second = scan_secrets(
        paths=(root,),
        as_of=date(2026, 10, 9),
        cache_path=cache_path,
        now=SCAN_TIME + timedelta(days=1),
    )

    assert first.scan_mode == SecretScanMode.FULL
    assert first.full_scan_reason == "缓存不存在"
    assert first.read_file_count == 3
    assert second.scan_mode == SecretScanMode.INCREMENTAL
    assert second.full_scan_reason is None
    assert (second.scanned_file_count, second.read_file_count, second.reused_file_count) == (
        3,
        0,
        3,
    )
    assert second.last_full_scan_at == SCAN_TIME
    assert second.status == "PASS"
    markdown = render_secret_scan_report(second)
    assert "- 扫描文件数：3" in markdown
    assert "- 扫描方式：增量" in markdown
    assert "- 本次读取文件数：0；沿用上次结果：3" in markdown


def test_incremental_scan_rereads_changed_file_and_reports_secret(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    paths = _write_tree(root)
    cache_path = tmp_path / "scan_cache.json"
    scan_secrets(paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME)
    secret_value = "DUMMY_SECRET_VALUE_NOT_A_REAL_KEY"
    paths[1].write_text(f"# report 1\napi_key: {secret_value}\n", encoding="utf-8")
    new_file = root / "new_report.json"
    new_file.write_text('{"note": "clean"}\n', encoding="utf-8")

    report = scan_secrets(
        paths=(root,), as_of=date(2026, 10, 10), cache_path=cache_path, now=SCAN_TIME
    )

    assert report.scan_mode == SecretScanMode.INCREMENTAL
    assert (report.scanned_file_count, report.read_file_count, report.reused_file_count) == (
        4,
        2,
        2,
    )
    assert report.status == "FAIL"
    assert [(finding.path, finding.line_number) for finding in report.findings] == [(paths[1], 2)]
    assert secret_value not in render_secret_scan_report(report)


def test_incremental_scan_keeps_reporting_unchanged_file_with_finding(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    _write_tree(root, file_count=1)
    leaked = root / "leak.yaml"
    leaked.write_text("token: TEST_ONLY_NON_PROVIDER_SECRET_TOKEN\n", encoding="utf-8")
    cache_path = tmp_path / "scan_cache.json"

    first = scan_secrets(
        paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME
    )
    second = scan_secrets(
        paths=(root,), as_of=date(2026, 10, 10), cache_path=cache_path, now=SCAN_TIME
    )

    assert first.error_count == 1
    assert second.scan_mode == SecretScanMode.INCREMENTAL
    assert second.error_count == 1
    assert (second.read_file_count, second.reused_file_count) == (1, 1)
    cached = json.loads(cache_path.read_text(encoding="utf-8"))["entries"]
    assert str(leaked) not in cached


def test_incremental_scan_drops_deleted_files_from_cache(tmp_path: Path) -> None:
    root = tmp_path / "outputs"
    paths = _write_tree(root)
    cache_path = tmp_path / "scan_cache.json"
    scan_secrets(paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME)
    paths[0].unlink()

    report = scan_secrets(
        paths=(root,), as_of=date(2026, 10, 10), cache_path=cache_path, now=SCAN_TIME
    )

    assert (report.scanned_file_count, report.read_file_count, report.reused_file_count) == (
        2,
        0,
        2,
    )
    cached = json.loads(cache_path.read_text(encoding="utf-8"))["entries"]
    assert sorted(cached) == sorted(str(path) for path in paths[1:])


@pytest.mark.parametrize(
    ("change", "expected_reason"),
    [
        ("force", "显式要求全量扫描"),
        ("corrupt", "缓存无法读取或格式不符"),
        ("schema", "缓存无法读取或格式不符"),
        ("rules", "扫描规则代码已变化"),
        ("scope", "扫描入口已变化"),
        ("stale", "距上次全量扫描已满 7 天"),
        ("future", "上次全量扫描时间晚于当前时间"),
    ],
)
def test_incremental_scan_falls_back_to_full_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
    expected_reason: str,
) -> None:
    root = tmp_path / "outputs"
    paths = _write_tree(root)
    other_root = tmp_path / "docs"
    _write_tree(other_root, file_count=1)
    cache_path = tmp_path / "scan_cache.json"
    scan_secrets(paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME)
    secret_value = "TEST_ONLY_NON_PROVIDER_SECRET_XY"
    # An edit that keeps size and mtime is invisible to the cache; only a full scan finds it.
    _rewrite_keeping_size_and_mtime(paths[0], f"api_key: {secret_value}")
    scan_paths: tuple[Path, ...] = (root,)
    now = SCAN_TIME + timedelta(days=1)
    force_full_scan = False
    if change == "force":
        force_full_scan = True
    elif change == "corrupt":
        cache_path.write_text("{not json", encoding="utf-8")
    elif change == "schema":
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
        payload["schema_version"] = "secret_scan_cache.v0"
        cache_path.write_text(json.dumps(payload), encoding="utf-8")
    elif change == "rules":
        monkeypatch.setattr(secret_hygiene, "_scan_rules_sha256", lambda: "0" * 64)
    elif change == "scope":
        scan_paths = (root, other_root)
    elif change == "stale":
        now = SCAN_TIME + timedelta(days=7)
    elif change == "future":
        now = SCAN_TIME - timedelta(minutes=1)

    report = scan_secrets(
        paths=scan_paths,
        as_of=date(2026, 10, 10),
        cache_path=cache_path,
        force_full_scan=force_full_scan,
        now=now,
    )

    assert report.scan_mode == SecretScanMode.FULL
    assert report.full_scan_reason == expected_reason
    assert report.reused_file_count == 0
    assert report.read_file_count == report.scanned_file_count
    assert report.last_full_scan_at == now
    assert report.status == "FAIL"
    assert f"- 扫描方式：全量（{expected_reason}）" in render_secret_scan_report(report)
    rebuilt = json.loads(cache_path.read_text(encoding="utf-8"))
    assert rebuilt["last_full_scan_at"] == now.isoformat()


def test_incremental_scan_misses_same_size_same_mtime_edit_until_full_scan(
    tmp_path: Path,
) -> None:
    root = tmp_path / "outputs"
    paths = _write_tree(root)
    cache_path = tmp_path / "scan_cache.json"
    scan_secrets(paths=(root,), as_of=date(2026, 10, 9), cache_path=cache_path, now=SCAN_TIME)
    _rewrite_keeping_size_and_mtime(paths[0], "api_key: TEST_ONLY_NON_PROVIDER_SECRET_XY")

    within_week = scan_secrets(
        paths=(root,),
        as_of=date(2026, 10, 10),
        cache_path=cache_path,
        now=SCAN_TIME + timedelta(days=6, hours=23),
    )
    after_week = scan_secrets(
        paths=(root,),
        as_of=date(2026, 10, 16),
        cache_path=cache_path,
        now=SCAN_TIME + timedelta(days=7),
    )

    # Documented limit of the cache: covered by the weekly full scan.
    assert within_week.status == "PASS"
    assert after_week.scan_mode == SecretScanMode.FULL
    assert after_week.status == "FAIL"


def test_secret_scan_reports_line_number_in_large_file(tmp_path: Path) -> None:
    source = tmp_path / "large.csv"
    lines = [f"row_{index},value_{index}" for index in range(500)]
    lines[321] = "authorization: Bearer TEST_ONLY_NON_PROVIDER_BEARER_TOKEN"
    source.write_text("\n".join(lines) + "\n", encoding="utf-8")
    clean = tmp_path / "clean.csv"
    clean.write_text("token,value\nshort,1\n", encoding="utf-8")

    report = scan_secrets(paths=(tmp_path,), as_of=date(2026, 10, 9))

    assert report.read_file_count == 2
    assert [(finding.path, finding.line_number) for finding in report.findings] == [(source, 322)]
    assert report.findings[0].matched_label == "Bearer"


def test_security_scan_secrets_cli_default_scope_uses_incremental_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    for directory in ("config", "docs", "outputs"):
        _write_tree(tmp_path / directory, file_count=2)
    cache_path = tmp_path / "data" / "cache" / "secret_hygiene" / "scan_cache.json"
    monkeypatch.setattr(secret_hygiene, "DEFAULT_SECRET_SCAN_CACHE_PATH", cache_path)
    output_path = tmp_path / "secret_hygiene.md"
    arguments = [
        "security",
        "scan-secrets",
        "--as-of",
        "2026-10-09",
        "--output-path",
        str(output_path),
    ]

    first = CliRunner().invoke(app, arguments)
    second = CliRunner().invoke(app, arguments)
    forced = CliRunner().invoke(app, [*arguments, "--full-scan"])

    assert first.exit_code == 0, first.output
    assert "扫描方式：全量；本次读取：6；沿用上次结果：0" in first.output
    assert second.exit_code == 0, second.output
    assert "扫描方式：增量；本次读取：0；沿用上次结果：6" in second.output
    assert forced.exit_code == 0, forced.output
    assert "扫描方式：全量；本次读取：6；沿用上次结果：0" in forced.output
    assert "- 扫描方式：全量（显式要求全量扫描）" in output_path.read_text(encoding="utf-8")
    assert cache_path.exists()


def test_security_scan_secrets_cli_custom_scope_does_not_use_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_path = tmp_path / "default_cache" / "scan_cache.json"
    monkeypatch.setattr(secret_hygiene, "DEFAULT_SECRET_SCAN_CACHE_PATH", cache_path)
    scan_root = tmp_path / "replay"
    _write_tree(scan_root)
    output_path = tmp_path / "secret_hygiene.md"

    result = CliRunner().invoke(
        app,
        [
            "security",
            "scan-secrets",
            "--as-of",
            "2026-10-09",
            "--scan-paths",
            str(scan_root),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "扫描方式：全量；本次读取：3；沿用上次结果：0" in result.output
    assert "- 扫描方式：全量（未启用增量缓存）" in output_path.read_text(encoding="utf-8")
    assert not cache_path.parent.exists()
