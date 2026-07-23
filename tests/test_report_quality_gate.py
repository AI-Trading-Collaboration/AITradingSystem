from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.reports import reader_brief
from ai_trading_system.reports.report_index import (
    DEFAULT_REPORT_REGISTRY_PATH,
    load_report_registry,
)
from ai_trading_system.reports.report_quality_gate import (
    build_report_quality_gate_payload,
    render_report_quality_gate_markdown,
)

RUN_DATE = date(2026, 5, 4)


def test_report_quality_gate_passes_complete_report_and_reader_brief(tmp_path: Path) -> None:
    report_path = _write_complete_report(tmp_path / "good_report.json")
    payload = build_report_quality_gate_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index(report_path),
        report_index_path=tmp_path / "report_index.json",
        reader_brief_payload=_complete_reader_brief_payload(),
        reader_brief_json_path=tmp_path / "reader_brief.json",
        project_root=tmp_path,
    )
    markdown = render_report_quality_gate_markdown(payload)

    assert payload["report_quality_status"] == "PASS"
    assert payload["summary"]["checked_report_count"] == 1
    assert payload["missing_sections"] == []
    assert payload["blocking_quality_issues"] == []
    assert "Report Quality Gate" in markdown


def test_report_quality_gate_warns_on_missing_report_sections(tmp_path: Path) -> None:
    report_path = tmp_path / "thin_report.json"
    _write_json(
        report_path,
        {
            "report_type": "thin_report",
            "status": "PASS",
            "production_effect": "none",
            "source_artifacts": [{"path": "input.json"}],
            "safety_boundary": {"production_effect": "none"},
        },
    )

    payload = build_report_quality_gate_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index(report_path),
        report_index_path=tmp_path / "report_index.json",
        reader_brief_payload=_complete_reader_brief_payload(),
        reader_brief_json_path=tmp_path / "reader_brief.json",
        project_root=tmp_path,
    )

    missing = {(item["report_id"], item["section"]) for item in payload["missing_sections"]}
    assert payload["report_quality_status"] == "PASS_WITH_WARNINGS"
    assert ("sample_report", "purpose") in missing
    assert ("sample_report", "limitations") in missing
    assert payload["summary"]["warning_quality_issue_count"] >= 1


def test_report_quality_gate_blocks_production_effect_risk(tmp_path: Path) -> None:
    report_path = _write_complete_report(
        tmp_path / "unsafe_report.json",
        production_effect="mutates",
    )

    payload = build_report_quality_gate_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index(report_path, production_effect="mutates"),
        report_index_path=tmp_path / "report_index.json",
        reader_brief_payload=_complete_reader_brief_payload(),
        reader_brief_json_path=tmp_path / "reader_brief.json",
        project_root=tmp_path,
    )

    assert payload["report_quality_status"] == "FAIL"
    assert any(
        issue["issue_id"] == "sample_report_production_effect_risk"
        for issue in payload["blocking_quality_issues"]
    )


def test_report_quality_gate_blocks_reader_brief_missing_core_sections(tmp_path: Path) -> None:
    report_path = _write_complete_report(tmp_path / "good_report.json")
    reader_payload = {
        "status": "PASS",
        "production_effect": "none",
        "warnings": [],
        "missing_limited_artifact_impact": {"status": "OK", "blocking_count": 0},
    }

    payload = build_report_quality_gate_payload(
        as_of=RUN_DATE,
        report_index_payload=_report_index(report_path),
        report_index_path=tmp_path / "report_index.json",
        reader_brief_payload=reader_payload,
        reader_brief_json_path=tmp_path / "reader_brief.json",
        project_root=tmp_path,
    )

    assert payload["report_quality_status"] == "FAIL"
    assert any(
        issue["section"] == "human_readable_summary"
        for issue in payload["blocking_quality_issues"]
    )
    assert any(
        issue["section"] == "recommended_next_step"
        for issue in payload["blocking_quality_issues"]
    )


def test_report_quality_gate_cli_writes_json_and_markdown(tmp_path: Path) -> None:
    report_path = _write_complete_report(tmp_path / "good_report.json")
    index_path = tmp_path / "report_index_2026-05-04.json"
    brief_path = tmp_path / "reader_brief_2026-05-04.json"
    json_output = tmp_path / "report_quality_gate_2026-05-04.json"
    markdown_output = tmp_path / "report_quality_gate_2026-05-04.md"
    _write_json(index_path, _report_index(report_path))
    _write_json(brief_path, _complete_reader_brief_payload())

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "quality-gate",
            "--date",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--report-index-path",
            str(index_path),
            "--reader-brief-json-path",
            str(brief_path),
            "--json-output-path",
            str(json_output),
            "--markdown-output-path",
            str(markdown_output),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(json_output.read_text(encoding="utf-8"))
    assert payload["report_type"] == "report_quality_gate"
    assert payload["report_quality_status"] == "PASS"
    assert markdown_output.exists()


def test_report_quality_gate_cli_keeps_warning_status_non_blocking(tmp_path: Path) -> None:
    report_path = tmp_path / "thin_report.json"
    _write_json(
        report_path,
        {
            "report_type": "thin_report",
            "status": "PASS",
            "production_effect": "none",
            "source_artifacts": [{"path": "input.json"}],
            "safety_boundary": {"production_effect": "none"},
        },
    )
    index_path = tmp_path / "report_index_2026-05-04.json"
    brief_path = tmp_path / "reader_brief_2026-05-04.json"
    json_output = tmp_path / "report_quality_gate_2026-05-04.json"
    markdown_output = tmp_path / "report_quality_gate_2026-05-04.md"
    _write_json(index_path, _report_index(report_path))
    _write_json(brief_path, _complete_reader_brief_payload())

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "quality-gate",
            "--date",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--report-index-path",
            str(index_path),
            "--reader-brief-json-path",
            str(brief_path),
            "--json-output-path",
            str(json_output),
            "--markdown-output-path",
            str(markdown_output),
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(json_output.read_text(encoding="utf-8"))
    assert payload["report_quality_status"] == "PASS_WITH_WARNINGS"
    assert markdown_output.exists()


def test_report_quality_gate_cli_fails_after_writing_failure_artifacts(
    tmp_path: Path,
) -> None:
    report_path = _write_complete_report(
        tmp_path / "unsafe_report.json",
        production_effect="mutates",
    )
    index_path = tmp_path / "report_index_2026-05-04.json"
    brief_path = tmp_path / "reader_brief_2026-05-04.json"
    json_output = tmp_path / "report_quality_gate_2026-05-04.json"
    markdown_output = tmp_path / "report_quality_gate_2026-05-04.md"
    _write_json(index_path, _report_index(report_path, production_effect="mutates"))
    _write_json(brief_path, _complete_reader_brief_payload())

    result = CliRunner().invoke(
        app,
        [
            "reports",
            "quality-gate",
            "--date",
            RUN_DATE.isoformat(),
            "--project-root",
            str(tmp_path),
            "--report-index-path",
            str(index_path),
            "--reader-brief-json-path",
            str(brief_path),
            "--json-output-path",
            str(json_output),
            "--markdown-output-path",
            str(markdown_output),
        ],
    )

    assert result.exit_code == 1
    assert "Report quality gate：FAIL" in result.output
    payload = json.loads(json_output.read_text(encoding="utf-8"))
    assert payload["report_quality_status"] == "FAIL"
    assert markdown_output.exists()


def test_report_quality_gate_registry_and_reader_brief_summary(tmp_path: Path) -> None:
    registry = load_report_registry(DEFAULT_REPORT_REGISTRY_PATH)
    assert any(item["report_id"] == "report_quality_gate" for item in registry["reports"])

    quality_path = tmp_path / "report_quality_gate_2026-05-04.json"
    _write_json(
        quality_path,
        {
            "report_type": "report_quality_gate",
            "report_quality_status": "PASS_WITH_WARNINGS",
            "status": "PASS_WITH_WARNINGS",
            "production_effect": "none",
            "next_action": "review_warning_quality_issues",
            "summary": {
                "checked_report_count": 3,
                "checked_reader_brief_count": 1,
                "missing_section_count": 2,
                "blocking_quality_issue_count": 0,
                "warning_quality_issue_count": 2,
            },
        },
    )
    summary = reader_brief._report_quality_gate_summary(
        {
            "reports": [
                {
                    "report_id": "report_quality_gate",
                    "latest_artifact_path": str(quality_path),
                    "exists": True,
                }
            ]
        }
    )

    assert summary["availability"] == "AVAILABLE"
    assert summary["report_quality_status"] == "PASS_WITH_WARNINGS"
    assert summary["missing_section_count"] == 2
    assert summary["detail_report"] == str(quality_path)


def _write_complete_report(path: Path, production_effect: str = "none") -> Path:
    _write_json(
        path,
        {
            "report_type": "sample_report",
            "purpose": "Validate sample report quality.",
            "source_artifacts": [{"path": "input.json"}],
            "output_decision": "PASS",
            "safety_boundary": {
                "production_effect": production_effect,
                "broker_action_allowed": False,
            },
            "limitations": ["fixture only"],
            "next_action": "continue_monitoring",
            "status": "PASS",
            "production_effect": production_effect,
        },
    )
    return path


def _complete_reader_brief_payload() -> dict[str, object]:
    return {
        "report_type": "reader_brief",
        "status": "PASS",
        "production_effect": "none",
        "narrative_executive_summary": {
            "today_conclusion": "系统保持 observation-only。",
            "why_this_conclusion": "质量门禁样例完整。",
        },
        "status_panel": {
            "build_status": "PASS",
            "decision_usability": "USABLE",
            "research_promotion_status": "NOT_PROMOTABLE",
        },
        "missing_limited_artifact_impact": {
            "status": "OK",
            "blocking_count": 0,
            "items": [],
        },
        "manual_review_queue": {"items": []},
        "warnings": [],
        "action_checklist": [{"recommended_next_action": "review_report_quality_gate"}],
        "executive_decision": {"not_trade_instruction": True},
    }


def _report_index(path: Path, production_effect: str = "none") -> dict[str, object]:
    return {
        "report_type": "report_index",
        "status": "PASS",
        "production_effect": "none",
        "summary": {"report_count": 1},
        "reports": [
            {
                "report_id": "sample_report",
                "title": "Sample Report",
                "latest_artifact_path": str(path),
                "exists": True,
                "freshness_status": "FRESH",
                "artifact_status": "PASS",
                "artifact_production_effect": production_effect,
            }
        ],
    }


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
