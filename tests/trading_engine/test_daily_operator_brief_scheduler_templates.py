from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ai_trading_system.trading_engine.daily_operator_brief_scheduler_templates import (
    build_daily_operator_brief_scheduler_templates_payload,
    render_scheduler_template_summary_markdown,
    scan_scheduler_template_safety,
    write_daily_operator_brief_scheduler_templates,
)


def test_scheduler_templates_default_generation_writes_all_templates(tmp_path: Path) -> None:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["template_generation_status"] == "GENERATED"
    assert payload["summary_level"] == "NORMAL"
    assert payload["generated_template_count"] == 5
    assert payload["safety_validation"]["status"] == "PASS"
    assert payload["template_inputs"]["expected_run_time_local"] == "09:00"
    assert payload["template_inputs"]["timezone"] == "Asia/Tokyo"

    output_templates = payload["output_templates"]
    expected_keys = {
        "windows_task_xml",
        "powershell_wrapper",
        "batch_wrapper",
        "cron_line",
        "github_actions_workflow",
    }
    assert set(output_templates) == expected_keys
    for record in output_templates.values():
        path = tmp_path / record["path"]
        assert record["enabled"] is True
        assert record["generated"] is True
        assert path.exists()
        assert path.name.endswith(".template")
        text = path.read_text(encoding="utf-8")
        assert "TEMPLATE ONLY" in text
        assert "Manual review required" in text

    metadata_path = tmp_path / payload["output_artifacts"]["metadata_json"]["path"]
    markdown_path = tmp_path / payload["output_artifacts"]["summary_markdown"]["path"]
    assert metadata_path.exists()
    assert markdown_path.exists()


def test_scheduler_templates_content_is_template_only_and_safe(tmp_path: Path) -> None:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    templates = {
        key: (tmp_path / record["path"]).read_text(encoding="utf-8")
        for key, record in payload["output_templates"].items()
    }
    scan = scan_scheduler_template_safety(templates)
    assert scan["status"] == "PASS"
    combined = "\n".join(templates.values()).lower()
    assert "scripts/run_shadow_promotion_apply.py" not in combined
    assert "scripts/run_shadow_promotion_rollback.py" not in combined
    assert "schtasks /create" not in combined
    assert "crontab -" not in combined
    assert "trading execution" not in combined
    for record in payload["output_templates"].values():
        assert ".github/workflows/" not in record["path"].replace("\\", "/")


def test_scheduler_templates_metadata_safety_boundaries(tmp_path: Path) -> None:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["scheduler_created"] is False
    assert payload["scheduler_installed"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["operator_brief_executed_by_template_generator"] is False
    assert payload["pipelines_executed_by_template_generator"] is False
    assert payload["data_downloaded_by_template_generator"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
    assert payload["manual_review_required"]["required"] is True


def test_scheduler_templates_dangerous_template_content_is_safety_blocked(
    tmp_path: Path,
) -> None:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
        template_overrides={
            "cron_line": "\n".join(
                [
                    "# TRADING-028 TEMPLATE ONLY.",
                    "# Manual review required before use.",
                    "0 9 * * * python scripts/run_shadow_promotion_apply.py",
                    "",
                ]
            )
        },
    )

    _assert_invariants(payload)
    assert payload["template_generation_status"] == "SAFETY_BLOCKED"
    assert payload["summary_level"] == "SAFETY_BLOCKED"
    assert payload["generated_template_count"] == 0
    assert payload["safety_validation"]["status"] == "FAIL"
    assert payload["safety_validation"]["blocking_reasons"]
    for record in payload["output_templates"].values():
        assert not (tmp_path / record["path"]).exists()


def test_scheduler_templates_github_workflow_output_path_is_safety_blocked(
    tmp_path: Path,
) -> None:
    unsafe_output = tmp_path / ".github" / "workflows"
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        output_root=unsafe_output,
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["template_generation_status"] == "SAFETY_BLOCKED"
    assert payload["generated_template_count"] == 0
    assert payload["safety_validation"]["blocking_reasons"]
    assert not unsafe_output.exists()


def test_scheduler_templates_system_scheduler_output_path_is_safety_blocked(
    tmp_path: Path,
) -> None:
    payload = build_daily_operator_brief_scheduler_templates_payload(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        output_root=Path("C:/Windows/System32/Tasks"),
        generated_at=_fixed_generated_at(),
    )

    _assert_invariants(payload)
    assert payload["template_generation_status"] == "SAFETY_BLOCKED"
    assert payload["generated_template_count"] == 0
    assert payload["safety_validation"]["blocking_reasons"]


def test_scheduler_templates_markdown_contains_required_sections(tmp_path: Path) -> None:
    payload = write_daily_operator_brief_scheduler_templates(
        as_of=date(2026, 5, 24),
        repo_root=tmp_path,
        python_path=tmp_path / ".venv" / "Scripts" / "python.exe",
        generated_at=_fixed_generated_at(),
    )
    markdown = render_scheduler_template_summary_markdown(payload)

    _assert_invariants(payload)
    assert "## 1. Summary" in markdown
    assert "## 2. Generated Templates" in markdown
    assert "## 3. Safety Statement" in markdown
    assert "## 4. Manual Review Checklist" in markdown
    assert "Scheduler Created: `false`" in markdown
    assert "Manual Review Required" in markdown


def _fixed_generated_at() -> datetime:
    return datetime(2026, 5, 24, 0, 0, tzinfo=UTC)


def _assert_invariants(payload: dict[str, Any]) -> None:
    assert payload["production_effect"] == "none"
    assert payload["manual_review_only"] is True
    assert payload["scheduler_template_only"] is True
    assert payload["read_only"] is True
    assert payload["scheduler_created"] is False
    assert payload["scheduler_installed"] is False
    assert payload["scheduler_enabled"] is False
    assert payload["operator_brief_executed_by_template_generator"] is False
    assert payload["pipelines_executed_by_template_generator"] is False
    assert payload["data_downloaded_by_template_generator"] is False
    assert payload["apply_executed_by_template_generator"] is False
    assert payload["rollback_executed_by_template_generator"] is False
    assert payload["broker_execution"] is False
    assert payload["replay_execution"] is False
    assert payload["trading_execution"] is False
