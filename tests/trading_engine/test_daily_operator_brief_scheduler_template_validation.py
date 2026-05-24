from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_template_validation import (
    build_daily_operator_brief_scheduler_template_validation_payload,
    write_daily_operator_brief_scheduler_template_validation,
)
from ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates import (
    write_daily_operator_brief_scheduler_templates,
)


def test_valid_trading_028_metadata_and_templates_pass_validation(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    _write_valid_fixture(tmp_path, as_of)

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "PASS"
    assert payload["coverage"]["templates_declared"] == 5
    assert payload["coverage"]["templates_found"] == 5
    assert payload["coverage"]["templates_passed"] == 5
    assert payload["input_artifacts"]["template_metadata"]["status"] == "FOUND"
    assert payload["input_artifacts"]["template_metadata"]["sha256"]


def test_metadata_missing_returns_input_missing(tmp_path: Path) -> None:
    payload = _build_validation(tmp_path, date(2026, 5, 24))

    _assert_invariants(payload)
    assert payload["validation_status"] == "INPUT_MISSING"
    assert payload["template_results"] == []


def test_invalid_metadata_json_returns_input_invalid(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _metadata_path(tmp_path, as_of)
    metadata_path.parent.mkdir(parents=True)
    metadata_path.write_text("{not-json", encoding="utf-8")

    payload = _build_validation(tmp_path, as_of, template_metadata_file=metadata_path)

    _assert_invariants(payload)
    assert payload["validation_status"] == "INPUT_INVALID"
    assert payload["alerts"]["critical"]


def test_metadata_task_id_must_be_trading_028(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _mutate_metadata(metadata_path, lambda metadata: metadata.__setitem__("task_id", "TRADING-999"))

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "INPUT_INVALID"


def test_metadata_safety_invalid_is_safety_blocked(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _mutate_metadata(
        metadata_path,
        lambda metadata: metadata.__setitem__("scheduler_enabled", True),
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["metadata_safe"] is False
    assert payload["safety_validation"]["blocking_reasons"]


def test_declared_template_missing_fails_validation(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    metadata = _read_json(metadata_path)
    missing = tmp_path / metadata["output_templates"]["cron_line"]["path"]
    missing.unlink()

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert payload["coverage"]["templates_missing"] == 1
    assert _result(payload, "cron_line")["status"] == "MISSING"


def test_template_suffix_must_remain_template(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    metadata = _read_json(metadata_path)
    original_path = tmp_path / metadata["output_templates"]["powershell_wrapper"]["path"]
    unsafe_suffix = original_path.with_suffix("")
    unsafe_suffix.write_text(original_path.read_text(encoding="utf-8"), encoding="utf-8")
    metadata["output_templates"]["powershell_wrapper"]["path"] = str(
        unsafe_suffix.relative_to(tmp_path).as_posix()
    )
    _write_json(metadata_path, metadata)

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert _result(payload, "powershell_wrapper")["suffix_valid"] is False


def test_template_path_outside_allowed_root_is_safety_blocked(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    unsafe_path = tmp_path / "outputs" / "run_daily_operator_brief.ps1.template"
    unsafe_path.parent.mkdir(parents=True)
    unsafe_path.write_text(_safe_script_text(), encoding="utf-8")
    _mutate_metadata(
        metadata_path,
        lambda metadata: metadata["output_templates"]["powershell_wrapper"].__setitem__(
            "path", "outputs/run_daily_operator_brief.ps1.template"
        ),
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "SAFETY_BLOCKED"
    assert _result(payload, "powershell_wrapper")["status"] == "SAFETY_BLOCKED"


def test_invalid_windows_xml_template_fails(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(metadata_path, "windows_task_xml", "<Task><Actions><Exec></Task>")

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert _result(payload, "windows_task_xml")["syntax_status"] == "FAIL"


def test_invalid_github_actions_yaml_template_fails(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(
        metadata_path,
        "github_actions_workflow",
        "# TRADING-028 TEMPLATE ONLY.\n# Manual review required.\nname: [unterminated\n",
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert _result(payload, "github_actions_workflow")["syntax_status"] == "FAIL"


def test_invalid_cron_line_template_fails(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(
        metadata_path,
        "cron_line",
        "\n".join(
            [
                "# TRADING-028 TEMPLATE ONLY.",
                "# Manual review required before use.",
                "0 9 * * python scripts/run_daily_operator_brief_scheduler_dry_run.py",
                "",
            ]
        ),
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert _result(payload, "cron_line")["syntax_status"] == "FAIL"


@pytest.mark.parametrize(
    "dangerous_line",
    [
        "python scripts/run_shadow_promotion_apply.py",
        "python scripts/run_shadow_promotion_rollback.py",
        "schtasks /Create /TN DailyOperatorBrief",
        "crontab - < cron.txt",
        "broker execution",
        "replay runner",
        "trading execution",
    ],
)
def test_dangerous_template_commands_are_safety_blocked(
    tmp_path: Path,
    dangerous_line: str,
) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(
        metadata_path,
        "batch_wrapper",
        "\n".join(
            [
                "REM TRADING-028 TEMPLATE ONLY.",
                "REM Manual review required before use.",
                dangerous_line,
                "",
            ]
        ),
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "SAFETY_BLOCKED"
    assert _result(payload, "batch_wrapper")["status"] == "SAFETY_BLOCKED"
    assert payload["safety_validation"]["blocking_reasons"]


@pytest.mark.parametrize(
    "template_text",
    [
        (
            "# Manual review required before use.\n"
            "python scripts/run_daily_operator_brief_scheduler_dry_run.py\n"
        ),
        (
            "# TRADING-028 TEMPLATE ONLY.\n"
            "python scripts/run_daily_operator_brief_scheduler_dry_run.py\n"
        ),
    ],
)
def test_script_templates_require_safety_text(tmp_path: Path, template_text: str) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(metadata_path, "powershell_wrapper", template_text)

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "FAIL"
    assert _result(payload, "powershell_wrapper")["status"] == "FAIL"


def test_placeholder_path_only_passes_with_warnings(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    metadata_path = _write_valid_fixture(tmp_path, as_of)
    _overwrite_template(
        metadata_path,
        "powershell_wrapper",
        "\n".join(
            [
                "# TRADING-028 TEMPLATE ONLY.",
                "# Manual review required before use.",
                '$RepoRoot = "C:\\path\\to\\AITradingSystem"',
                'python scripts/run_daily_operator_brief_scheduler_dry_run.py --date "2026-05-24"',
                "",
            ]
        ),
    )

    payload = _build_validation(tmp_path, as_of)

    _assert_invariants(payload)
    assert payload["validation_status"] == "PASS_WITH_WARNINGS"
    assert payload["alerts"]["warnings"]
    assert _result(payload, "powershell_wrapper")["status"] == "WARNING"


def test_validator_writes_json_markdown_and_run_logs(tmp_path: Path) -> None:
    as_of = date(2026, 5, 24)
    _write_valid_fixture(tmp_path, as_of)

    payload = write_daily_operator_brief_scheduler_template_validation(
        as_of=as_of,
        repo_root=tmp_path,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["validation_status"] == "PASS"
    for artifact in payload["output_artifacts"].values():
        assert (tmp_path / artifact["path"]).exists()
    run_log = json.loads(
        (tmp_path / payload["output_artifacts"]["run_log_json"]["path"]).read_text(encoding="utf-8")
    )
    _assert_invariants(run_log)
    assert run_log["validation_status"] == "PASS"


def _write_valid_fixture(tmp_path: Path, as_of: date) -> Path:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=as_of,
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
    )
    return tmp_path / payload["output_artifacts"]["metadata_json"]["path"]


def _build_validation(
    tmp_path: Path,
    as_of: date,
    *,
    template_metadata_file: Path | None = None,
) -> dict[str, Any]:
    return build_daily_operator_brief_scheduler_template_validation_payload(
        as_of=as_of,
        repo_root=tmp_path,
        template_metadata_file=template_metadata_file,
        generated_at=_fixed_generated_at(),
    )


def _metadata_path(tmp_path: Path, as_of: date) -> Path:
    return (
        tmp_path
        / "data"
        / "derived"
        / "operator_briefs"
        / "scheduler_templates"
        / f"daily_operator_brief_scheduler_templates_{as_of.isoformat()}.json"
    )


def _overwrite_template(metadata_path: Path, template_id: str, text: str) -> None:
    metadata = _read_json(metadata_path)
    template_path = metadata_path.parents[4] / metadata["output_templates"][template_id]["path"]
    template_path.write_text(text, encoding="utf-8")


def _mutate_metadata(metadata_path: Path, mutator: Any) -> None:
    metadata = _read_json(metadata_path)
    mutator(metadata)
    _write_json(metadata_path, metadata)


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _result(payload: dict[str, Any], template_id: str) -> dict[str, Any]:
    return next(
        result for result in payload["template_results"] if result["template_id"] == template_id
    )


def _safe_script_text() -> str:
    return "\n".join(
        [
            "# TRADING-028 TEMPLATE ONLY.",
            "# Manual review required before use.",
            "python scripts/run_daily_operator_brief_scheduler_dry_run.py --date 2026-05-24",
            "",
        ]
    )


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, 0, 0, tzinfo=UTC)


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["scheduler_template_validation_only"] is True
    assert payload["read_only"] is True
    assert payload["scheduler_created"] is False
    assert payload["scheduler_installed"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["templates_executed_by_validator"] is False
    assert payload["operator_brief_executed_by_validator"] is False
    assert payload["pipelines_executed_by_validator"] is False
    assert payload["data_downloaded_by_validator"] is False
    assert payload["apply_executed_by_validator"] is False
    assert payload["rollback_executed_by_validator"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
