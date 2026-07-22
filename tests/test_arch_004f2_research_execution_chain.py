from __future__ import annotations

from pathlib import Path

DOCUMENT = Path("docs/research/current_research_strategy_execution_chain.md")


def test_research_execution_chain_documents_semantics_calculation_and_optimization() -> None:
    text = DOCUMENT.read_text(encoding="utf-8")

    required_sections = (
        "## 3. 为什么这样设计",
        "## 4. End-to-end 研究链路",
        "## 5. 逐环节输入、输出与计算逻辑",
        "## 6. 真实 reference trace",
        "## 7. 当前研究结果",
        "## 8. 定期复核与优化触发",
        "## 9. 优化空间与进入条件",
        "## 10. 当前架构缺口",
    )
    for section in required_sections:
        assert section in text

    assert "`unified_primary_2021`，起点 `2021-02-22`" in text
    assert "`exact_three_asset_validated`，起点 `2021-02-22`" in text
    assert "历史 regime" in text
    assert "周期复核不等于自动调优" in text
    assert "workflow PASS 不等于投资有效性 PASS" in text
    assert "GROWTH_TILT_CANDIDATE_FAMILY_CLOSED_NO_EXECUTABLE_PIT_CANDIDATE" in text
    assert "WEIGHT_RESEARCH_PROGRAM_NEEDS_MORE_EVIDENCE" in text

    required_calculations = (
        "desired_turnover = sum(abs(drift_i))",
        "strategy_return = gross_return - transaction_cost",
        "score = 100",
        "tilt_multiplier = 1 + offset * 0.25",
        "utility = total_return - 0.75 * abs(max_drawdown) - 0.25 * turnover",
        "total_return = product(1 + r_t) - 1",
    )
    for calculation in required_calculations:
        assert calculation in text

    required_sources = (
        "config/research/research_window_registry.yaml",
        "src/ai_trading_system/contracts/research_context.py",
        "src/ai_trading_system/research_framework/runner.py",
        "src/ai_trading_system/etf_portfolio/weight_research_b2.py",
        "src/ai_trading_system/backtest/robustness.py",
    )
    for source in required_sources:
        assert source in text


def test_research_execution_chain_keeps_production_boundary_explicit() -> None:
    text = DOCUMENT.read_text(encoding="utf-8")

    assert "不构成投资建议、策略晋升或交易授权" in text
    assert "`production_effect=none`" in text
    assert "official target weights、paper-shadow、production、broker 均未启用" in text
    assert "不能自动调参、改权重或 promotion" in text


def test_research_execution_chain_rejects_retired_2022_primary_claims() -> None:
    text = DOCUMENT.read_text(encoding="utf-8")

    retired_claims = (
        "当前 source-backed 结果仍使用 `ai_after_chatgpt=2022-12-01` 主结论窗口",
        "当前 source-backed 结果仍以 `ai_after_chatgpt=2022-12-01` 为主结论窗口",
        "主结论窗口仍是 `ai_after_chatgpt=2022-12-01`",
    )
    for claim in retired_claims:
        assert claim not in text

    assert "Active primary conclusion window 统一从 `2021-02-22` 开始" in text
    normalized = " ".join(text.split())
    assert normalized.count("不再是 active default、required comparator 或 minimum start") >= 3
    assert "immutable historical/legacy comparison" in text
