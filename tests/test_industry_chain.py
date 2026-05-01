from __future__ import annotations

from datetime import date
from pathlib import Path

from typer.testing import CliRunner

from ai_trading_system.cli import app
from ai_trading_system.config import (
    IndustryChainConfig,
    IndustryChainNodeConfig,
    load_industry_chain,
    load_watchlist,
)
from ai_trading_system.industry_chain import (
    render_industry_chain_validation_report,
    validate_industry_chain_config,
    write_industry_chain_validation_report,
)


def test_validate_industry_chain_config_passes_default_config() -> None:
    report = validate_industry_chain_config(
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.status == "PASS"
    assert report.error_count == 0
    assert len(report.nodes) >= 10


def test_validate_industry_chain_config_rejects_missing_watchlist_node() -> None:
    industry_chain = load_industry_chain()
    broken = IndustryChainConfig(
        nodes=[node for node in industry_chain.nodes if node.node_id != "gpu_asic_demand"],
    )

    report = validate_industry_chain_config(
        industry_chain=broken,
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "watchlist_node_missing" in {issue.code for issue in report.issues}


def test_validate_industry_chain_config_detects_cycles() -> None:
    industry_chain = IndustryChainConfig(
        nodes=[
            _node("a", parent_node_ids=["b"]),
            _node("b", parent_node_ids=["a"]),
        ],
    )

    report = validate_industry_chain_config(
        industry_chain=industry_chain,
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    assert report.passed is False
    assert "cycle_detected" in {issue.code for issue in report.issues}


def test_render_and_write_industry_chain_report(tmp_path: Path) -> None:
    report = validate_industry_chain_config(
        industry_chain=load_industry_chain(),
        watchlist=load_watchlist(),
        as_of=date(2026, 5, 2),
    )

    markdown = render_industry_chain_validation_report(report)
    output_path = write_industry_chain_validation_report(
        report,
        tmp_path / "industry_chain.md",
    )

    assert "- 状态：PASS" in markdown
    assert "cloud_capex" in markdown
    assert output_path.read_text(encoding="utf-8") == markdown


def test_industry_chain_cli_validate_writes_report(tmp_path: Path) -> None:
    output_path = tmp_path / "industry_chain_validation.md"

    result = CliRunner().invoke(
        app,
        [
            "industry-chain",
            "validate",
            "--as-of",
            "2026-05-02",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "产业链校验状态：PASS" in result.output


def test_industry_chain_cli_list_outputs_nodes() -> None:
    result = CliRunner().invoke(app, ["industry-chain", "list"])

    assert result.exit_code == 0
    assert "产业链因果图" in result.output
    assert "cloud_capex" in result.output


def _node(node_id: str, parent_node_ids: list[str]) -> IndustryChainNodeConfig:
    return IndustryChainNodeConfig(
        node_id=node_id,
        name=node_id,
        description="测试节点",
        parent_node_ids=parent_node_ids,
        leading_indicators=["测试指标"],
        related_tickers=["NVDA"],
        impact_horizon="medium",
        cash_flow_relevance="medium",
        sentiment_relevance="medium",
    )
