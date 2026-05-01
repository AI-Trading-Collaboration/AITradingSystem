# AI Trading System

面向美股 AI 产业链的趋势分析、风险评分、回测与仓位建议系统。

项目目标不是预测市场，也不是自动交易，而是把“是否加仓、减仓、观望”变成可复盘、可验证、可控制风险的决策流程。

## MVP 范围

第一版只做闭环：

1. 市场价格与宏观风险数据采集。
2. 趋势、相对强弱、波动率、利率等特征计算。
3. 规则评分模型。
4. 仓位区间建议。
5. 与 QQQ、SMH/SOXX、SPY 的回测对比。
6. 每日 Markdown 报告。

基本面、估值、新闻/NLP、LLM 事件抽取放到后续阶段。

## 工程结构

```text
config/                  投资标的池、模块权重、运行参数
data/raw/                原始数据缓存，不提交
data/processed/          清洗后的中间数据，不提交
data/external/           外部导入数据，不提交
docs/                    架构和开发计划
notebooks/               研究和临时分析
outputs/backtests/       回测输出，不提交
outputs/reports/         日报/周报输出，不提交
src/ai_trading_system/   应用代码
tests/                   单元测试
```

## 本地开发

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev,data,dashboard]"
python -m pytest
```

查看示例评分：

```powershell
aits score-example
```

示例报告会同时输出两个仓位口径：

- AI 仓位（股票风险资产内）。
- AI 仓位（总资产内），根据 `config/portfolio.yaml` 的风险资产预算换算。

## 投资边界

系统输出只作为个人研究和仓位管理辅助，不构成投资建议。所有策略都需要回测、复盘，并显式考虑税费、滑点、汇率、交易延迟和极端风险。
