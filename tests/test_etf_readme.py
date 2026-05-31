from __future__ import annotations

from pathlib import Path


def test_readme_documents_etf_operator_workflow_and_boundaries() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme.split())

    required_terms = [
        "## ETF 主仓组合配置系统",
        "config/etf_portfolio/",
        "aits etf validate-config",
        "aits etf data validate --date latest",
        "aits etf run daily --date latest --dry-run",
        "aits etf backtest run --fast",
        "aits data ingest/validate",
        "aits features build",
        "aits run daily",
        "aits experiments run/compare/register",
        "aits etf experiments run --config <candidate.yaml>",
        "aits etf experiments compare --baseline production",
        "data/etf_portfolio/target_weights.csv",
        "data/simulation/etf_ledger.csv",
        "reports/etf_portfolio/YYYY-MM-DD_portfolio_brief.md",
        "reports/etf_portfolio/backtests/<run_id>/summary.md",
        "ai_after_chatgpt",
        "2022-12-01",
        "data_quality_status",
        "model_version",
        "config_hash",
        "production_effect=none",
        "不触发 broker 或 trading action",
    ]

    missing = [term for term in required_terms if term not in normalized]
    assert not missing


def test_readme_keeps_etf_backtest_separate_from_root_backtest() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    normalized = " ".join(readme.split())

    assert "根级 `aits backtest` 已属于现有主系统每日评分回测" in normalized
    assert "ETF 回测继续使用" in normalized
    assert "aits etf backtest run/report" in normalized
