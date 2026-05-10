from __future__ import annotations

import csv
from datetime import UTC, date, datetime
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.official_policy_sources import OFFICIAL_POLICY_CANDIDATE_COLUMNS
from ai_trading_system.risk_event_candidate_triage import (
    load_triaged_candidate_ids,
    render_risk_event_candidate_triage_report,
    triage_official_policy_candidates,
    write_risk_event_candidate_triage_csv,
)


def test_triage_prioritizes_direct_ai_module_signals_and_lowers_broad_sanctions(
    tmp_path: Path,
) -> None:
    input_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    _write_candidates(
        input_path,
        [
            _row(
                candidate_id="official:fr:export-control",
                source_id="official_federal_register_policy_documents",
                source_title=(
                    "BIS updates Entity List rules for advanced computing semiconductors"
                ),
                matched_topics="export_controls;ai_policy",
                matched_risk_ids="ai_chip_export_control_upgrade",
                affected_tickers="NVDA;AMD;TSM;INTC",
            ),
            _row(
                candidate_id="official:congress:ai-report",
                source_id="official_congress_bills",
                source_title=(
                    "A bill to require a report on the use of artificial intelligence "
                    "by federal agencies"
                ),
                matched_topics="ai_policy",
                matched_risk_ids="ai_chip_export_control_upgrade",
            ),
            _row(
                candidate_id="official:ofac:bank",
                source_id="official_ofac_sdn_xml",
                source_title="VTB BANK PJSC",
                matched_topics="sanctions;russia_geopolitics",
                matched_risk_ids="ai_chip_export_control_upgrade",
                affected_tickers="NVDA;AMD;TSM;INTC",
            ),
            _row(
                candidate_id="official:ustr:section-301",
                source_id="official_ustr_press_releases",
                source_title="Section 301 investigation into China trade measures",
                matched_topics="trade_policy;china_technology",
            ),
        ],
    )

    report = triage_official_policy_candidates(
        input_path,
        as_of=date(2026, 5, 10),
        generated_at=datetime(2026, 5, 10, tzinfo=UTC),
    )
    buckets = {record.candidate_id: record.triage_bucket for record in report.records}

    assert report.status == "PASS"
    assert buckets["official:fr:export-control"] == "must_review"
    assert buckets["official:congress:ai-report"] == "review_next"
    assert buckets["official:ofac:bank"] == "auto_low_relevance"
    assert buckets["official:ustr:section-301"] == "sample_review"
    assert report.bucket_counts["must_review"] == 1
    assert report.bucket_counts["auto_low_relevance"] == 1


def test_triage_marks_duplicate_titles_without_losing_audit_row(tmp_path: Path) -> None:
    input_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    title = "BIS updates Entity List rules for advanced computing semiconductors"
    _write_candidates(
        input_path,
        [
            _row(candidate_id="official:fr:1", source_title=title),
            _row(candidate_id="official:bis:1", source_title=title),
        ],
    )

    report = triage_official_policy_candidates(input_path, as_of=date(2026, 5, 10))
    buckets = {record.candidate_id: record.triage_bucket for record in report.records}

    assert buckets["official:fr:1"] == "must_review"
    assert buckets["official:bis:1"] == "duplicate_or_noise"
    assert report.row_count == 2


def test_triage_report_and_csv_preserve_production_boundary(tmp_path: Path) -> None:
    input_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    output_path = tmp_path / "official_policy_candidate_triage_2026-05-10.csv"
    _write_candidates(input_path, [_row()])

    report = triage_official_policy_candidates(input_path, as_of=date(2026, 5, 10))
    markdown = render_risk_event_candidate_triage_report(report)
    write_risk_event_candidate_triage_csv(report, output_path)
    rows = list(csv.DictReader(output_path.open("r", newline="", encoding="utf-8")))

    assert "不会写入 `risk_event_occurrence`" in markdown
    assert "不会进入评分、仓位闸门或回测标签" in markdown
    assert rows[0]["triage_bucket"] == "must_review"
    assert rows[0]["production_effect"] == "none"
    assert rows[0]["ai_relevance_score"] == "95"


def test_load_triaged_candidate_ids_filters_selected_buckets(tmp_path: Path) -> None:
    input_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    output_path = tmp_path / "official_policy_candidate_triage_2026-05-10.csv"
    _write_candidates(
        input_path,
        [
            _row(candidate_id="official:high"),
            _row(
                candidate_id="official:low",
                source_id="official_ofac_sdn_xml",
                source_title="VTB BANK PJSC",
                matched_topics="sanctions",
            ),
        ],
    )
    report = triage_official_policy_candidates(input_path, as_of=date(2026, 5, 10))
    write_risk_event_candidate_triage_csv(report, output_path)

    candidate_ids = load_triaged_candidate_ids(
        output_path,
        buckets=("must_review", "review_next"),
    )

    assert candidate_ids == ("official:high",)


def test_risk_events_triage_official_candidates_cli_writes_outputs(tmp_path: Path) -> None:
    input_path = tmp_path / "official_policy_source_candidates_2026-05-10.csv"
    csv_path = tmp_path / "triage.csv"
    report_path = tmp_path / "triage.md"
    _write_candidates(input_path, [_row()])

    result = CliRunner().invoke(
        app,
        [
            "risk-events",
            "triage-official-candidates",
            "--input-path",
            str(input_path),
            "--triage-output-path",
            str(csv_path),
            "--output-path",
            str(report_path),
            "--as-of",
            "2026-05-10",
        ],
    )

    assert result.exit_code == 0
    assert "官方候选 AI 模块 triage 状态：PASS" in result.output
    assert "must_review=1" in result.output
    assert csv_path.exists()
    assert report_path.exists()


def test_risk_events_triage_official_candidates_cli_help() -> None:
    result = CliRunner().invoke(app, ["risk-events", "triage-official-candidates", "--help"])

    assert result.exit_code == 0
    assert "按 AI 模块相关性分类官方政策/地缘候选" in result.output


def _write_candidates(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OFFICIAL_POLICY_CANDIDATE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "candidate_id": "official:fr:default",
        "as_of": "2026-05-10",
        "source_id": "official_federal_register_policy_documents",
        "provider": "Federal Register API",
        "source_type": "primary_source",
        "source_name": "Federal Register API",
        "source_url": "https://www.federalregister.gov/d/2026-00001",
        "source_title": "BIS updates export controls for advanced computing chips",
        "published_at": "2026-05-10",
        "captured_at": "2026-05-10",
        "matched_topics": "export_controls;ai_policy",
        "matched_risk_ids": "ai_chip_export_control_upgrade",
        "affected_tickers": "NVDA;AMD;TSM;INTC",
        "affected_nodes": "export_controls;gpu_asic_demand",
        "evidence_grade_floor": "A",
        "review_status": "pending_review",
        "review_questions": "是否影响 AI 模块？",
        "raw_payload_path": "data/raw/official_policy_sources/2026-05-10/source.json",
        "raw_payload_sha256": "a" * 64,
        "row_count": 1,
        "production_effect": "none",
        "notes": "test",
    }
    row.update(overrides)
    return row
