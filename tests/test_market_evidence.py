from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.market_evidence import (
    import_market_evidence_csv,
    load_market_evidence_store,
    render_market_evidence_validation_report,
    validate_market_evidence_store,
)


def test_market_evidence_validation_passes_confirmed_primary_source(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "evidence.yaml"
    _write_evidence_yaml(
        evidence_path,
        source_type="primary_source",
        source_url="https://example.test/company-release",
        manual_review_status="confirmed",
        manual_review_required=False,
    )

    report = validate_market_evidence_store(
        load_market_evidence_store(evidence_path),
        as_of=date(2026, 5, 2),
    )
    markdown = render_market_evidence_validation_report(report)

    assert report.status == "PASS"
    assert report.confirmed_count == 1
    assert "nvda_capex_watch_2026_05_02" in markdown


def test_llm_market_evidence_is_forced_to_pending_review(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "llm_evidence.yaml"
    _write_evidence_yaml(
        evidence_path,
        evidence_id="llm_nvda_capex_2026_05_02",
        source_type="llm_extracted",
        source_url="",
        manual_review_status="confirmed",
        manual_review_required=False,
    )

    store = load_market_evidence_store(evidence_path)
    evidence = store.loaded[0].evidence
    report = validate_market_evidence_store(store, as_of=date(2026, 5, 2))

    assert evidence.manual_review_status == "pending_review"
    assert evidence.manual_review_required is True
    assert report.status == "PASS_WITH_WARNINGS"
    assert "llm_evidence_pending_review" in {issue.code for issue in report.issues}


def test_public_convenience_market_evidence_is_not_scoreable(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "public_evidence.yaml"
    _write_evidence_yaml(
        evidence_path,
        evidence_id="public_nvda_note_2026_05_02",
        source_type="public_convenience",
        source_url="https://example.test/public-note",
        manual_review_status="pending_review",
    )

    report = validate_market_evidence_store(
        load_market_evidence_store(evidence_path),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert "public_convenience_evidence_not_scoreable" in {
        issue.code for issue in report.issues
    }


def test_import_market_evidence_csv_round_trips_to_validation(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "market_evidence.csv"
    output_dir = tmp_path / "market_evidence"
    _write_evidence_csv(csv_path)

    import_report = import_market_evidence_csv(csv_path)
    cli_result = CliRunner().invoke(
        app,
        [
            "evidence",
            "import-csv",
            "--input-path",
            str(csv_path),
            "--output-dir",
            str(output_dir),
            "--report-path",
            str(tmp_path / "market_evidence_import.md"),
        ],
    )
    validation_result = CliRunner().invoke(
        app,
        [
            "evidence",
            "validate",
            "--input-path",
            str(output_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(tmp_path / "market_evidence_validation.md"),
        ],
    )

    assert import_report.status == "PASS"
    assert cli_result.exit_code == 0
    assert validation_result.exit_code == 0
    assert (output_dir / "nvda_capex_watch_2026_05_02.yaml").exists()
    assert "Market evidence 已导入" in cli_result.output
    assert "Market evidence 状态：PASS" in validation_result.output


def _write_evidence_yaml(
    path: Path,
    *,
    evidence_id: str = "nvda_capex_watch_2026_05_02",
    source_type: str,
    source_url: str,
    manual_review_status: str,
    manual_review_required: bool = True,
) -> None:
    path.write_text(
        f"""evidence_id: {evidence_id}
source_name: unit_test_source
source_type: {source_type}
source_url: "{source_url}"
published_at: 2026-05-01
captured_at: 2026-05-02
tickers:
  - NVDA
industry_chain_nodes:
  - gpu_asic_demand
topic: 云 CapEx 分歧观察
evidence_grade: B
novelty: new
impact_horizon: medium
direction: mixed
confidence: 0.7
manual_review_status: {manual_review_status}
manual_review_required: {str(manual_review_required).lower()}
linked_thesis: nvda_ai_infra_2026_q2
raw_summary: 测试用市场证据记录。
""",
        encoding="utf-8",
    )


def _write_evidence_csv(path: Path) -> None:
    path.write_text(
        "\n".join(
            [
                "evidence_id,source_name,source_type,source_url,published_at,captured_at,"
                "tickers,industry_chain_nodes,topic,evidence_grade,novelty,impact_horizon,"
                "direction,confidence,manual_review_status,manual_review_required,"
                "linked_thesis,raw_summary",
                "nvda_capex_watch_2026_05_02,manual_review,manual_input,,"
                "2026-05-01,2026-05-02,NVDA,gpu_asic_demand,云 CapEx 分歧观察,"
                "B,new,medium,mixed,0.7,confirmed,false,nvda_ai_infra_2026_q2,"
                "人工确认的测试证据。",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
