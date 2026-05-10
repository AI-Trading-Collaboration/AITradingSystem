from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import WatchlistConfig, load_industry_chain, load_watchlist
from ai_trading_system.thesis import (
    build_thesis_review_report,
    load_trade_thesis_store,
    render_thesis_review_report,
    render_thesis_validation_report,
    validate_trade_thesis_store,
    write_thesis_review_report,
    write_thesis_validation_report,
)


def test_validate_trade_thesis_store_passes_valid_thesis(tmp_path: Path) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(thesis_path)

    store = load_trade_thesis_store(thesis_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is True
    assert report.thesis_count == 1
    assert report.active_count == 1


def test_validate_trade_thesis_store_rejects_unknown_node(tmp_path: Path) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(thesis_path, node_id="unknown_node")

    store = load_trade_thesis_store(thesis_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "unknown_ai_chain_node" in {issue.code for issue in report.issues}


def test_validate_trade_thesis_store_rejects_triggered_active_thesis(tmp_path: Path) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(
        thesis_path,
        triggered=True,
        triggered_at="2026-05-01",
    )

    store = load_trade_thesis_store(thesis_path)
    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "triggered_falsification_condition_still_active" in {
        issue.code for issue in report.issues
    }


def test_validate_trade_thesis_store_accepts_warning_state_metadata(
    tmp_path: Path,
) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(
        thesis_path,
        status="warning",
        previous_status="active",
        status_updated_at="2026-05-02",
        status_reason="云 CapEx 指引出现分歧，需要降低结论确定性。",
        status_evidence="market_evidence:nvda_capex_watch_2026_05_02",
        manual_review_required=True,
    )

    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(thesis_path),
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_thesis_review_report(validation_report)
    markdown = render_thesis_validation_report(validation_report)

    assert validation_report.passed is True
    assert "missing_thesis_status_reason" not in {
        issue.code for issue in validation_report.issues
    }
    assert validation_report.active_count == 1
    assert review_report.items[0].health == "WATCH"
    assert "警告" in markdown
    assert "需要" in markdown


def test_validate_trade_thesis_store_rejects_invalid_status_transition(
    tmp_path: Path,
) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(
        thesis_path,
        status="active",
        previous_status="invalidated",
        status_updated_at="2026-05-02",
        status_reason="试图恢复已证伪 thesis。",
        status_evidence="manual_review",
    )

    report = validate_trade_thesis_store(
        store=load_trade_thesis_store(thesis_path),
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "invalid_thesis_status_transition" in {issue.code for issue in report.issues}


def test_validate_trade_thesis_store_allows_missing_path_for_watch_only_tickers(
    tmp_path: Path,
) -> None:
    store = load_trade_thesis_store(tmp_path / "missing_trade_theses")

    report = validate_trade_thesis_store(
        store=store,
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert "thesis_path_missing" not in {issue.code for issue in report.issues}


def test_validate_trade_thesis_store_warns_when_active_trade_requires_thesis(
    tmp_path: Path,
) -> None:
    watchlist = load_watchlist()
    active_trade_watchlist = WatchlistConfig(
        items=[
            item.model_copy(
                update={"decision_stage": "active_trade", "thesis_required": True}
            )
            if item.ticker == "NVDA"
            else item
            for item in watchlist.items
        ],
    )
    store = load_trade_thesis_store(tmp_path / "missing_trade_theses")

    report = validate_trade_thesis_store(
        store=store,
        watchlist=active_trade_watchlist,
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS_WITH_WARNINGS"
    assert "thesis_path_missing" in {issue.code for issue in report.issues}


def test_render_and_write_thesis_reports(tmp_path: Path) -> None:
    thesis_path = tmp_path / "nvda.yaml"
    _write_valid_thesis(thesis_path)
    validation_report = validate_trade_thesis_store(
        store=load_trade_thesis_store(thesis_path),
        watchlist=load_watchlist(),
        industry_chain=load_industry_chain(),
        as_of=date(2026, 5, 2),
    )
    review_report = build_thesis_review_report(validation_report)

    validation_markdown = render_thesis_validation_report(validation_report)
    review_markdown = render_thesis_review_report(review_report)
    validation_path = write_thesis_validation_report(
        validation_report,
        tmp_path / "validation.md",
    )
    review_path = write_thesis_review_report(review_report, tmp_path / "review.md")

    assert "- 状态：PASS" in validation_markdown
    assert "nvda_ai_infra_2026_q2" in validation_markdown
    assert "假设仍成立" in review_markdown
    assert validation_path.read_text(encoding="utf-8") == validation_markdown
    assert review_path.read_text(encoding="utf-8") == review_markdown


def test_thesis_cli_validate_review_and_list(tmp_path: Path) -> None:
    thesis_dir = tmp_path / "trade_theses"
    thesis_dir.mkdir()
    _write_valid_thesis(thesis_dir / "nvda.yaml")
    validation_path = tmp_path / "thesis_validation.md"
    review_path = tmp_path / "thesis_review.md"

    validate_result = CliRunner().invoke(
        app,
        [
            "thesis",
            "validate",
            "--input-path",
            str(thesis_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(validation_path),
        ],
    )
    review_result = CliRunner().invoke(
        app,
        [
            "thesis",
            "review",
            "--input-path",
            str(thesis_dir),
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(review_path),
        ],
    )
    list_result = CliRunner().invoke(
        app,
        [
            "thesis",
            "list",
            "--input-path",
            str(thesis_dir),
        ],
    )

    assert validate_result.exit_code == 0
    assert review_result.exit_code == 0
    assert list_result.exit_code == 0
    assert validation_path.exists()
    assert review_path.exists()
    assert "交易 thesis 校验状态：PASS" in validate_result.output
    assert "交易 thesis 复核状态：PASS" in review_result.output
    assert "NVDA" in list_result.output


def _write_valid_thesis(
    path: Path,
    node_id: str = "gpu_asic_demand",
    triggered: bool = False,
    triggered_at: str | None = None,
    status: str = "active",
    previous_status: str | None = None,
    status_updated_at: str | None = None,
    status_reason: str = "",
    status_evidence: str = "",
    manual_review_required: bool = False,
) -> None:
    triggered_at_line = f"    triggered_at: {triggered_at}\n" if triggered_at else ""
    previous_status_line = f"previous_status: {previous_status}\n" if previous_status else ""
    status_updated_at_line = (
        f"status_updated_at: {status_updated_at}\n" if status_updated_at else ""
    )
    path.write_text(
        f"""thesis_id: nvda_ai_infra_2026_q2
ticker: NVDA
direction: long
created_at: 2026-05-01
time_horizon: medium
{previous_status_line}{status_updated_at_line}status_reason: "{status_reason}"
status_evidence: "{status_evidence}"
manual_review_required: {str(manual_review_required).lower()}
position_scope: core_ai_bucket
entry_reason:
  - AI 基础设施需求仍在扩大，GPU 需求和生态优势尚未被证伪。
ai_chain_nodes:
  - {node_id}
validation_metrics:
  - metric_id: data_center_growth
    description: 数据中心收入和订单能否继续支撑 AI GPU 需求。
    evidence_source: manual_earnings_review
    expected_direction: growth_remains_strong
    latest_status: confirmed
    updated_at: 2026-05-01
falsification_conditions:
  - condition_id: capex_cut
    description: 云厂商 CapEx 指引大幅下修且管理层确认 AI 需求放缓。
    severity: high
    triggered: {str(triggered).lower()}
{triggered_at_line}risk_events:
  - risk_id: export_controls
    level: L2
    description: 出口限制升级可能影响部分区域收入。
    action: reduce_position_if_revenue_guidance_changes
    active: false
    updated_at: 2026-05-01
review_frequency: weekly
status: {status}
""",
        encoding="utf-8",
    )
