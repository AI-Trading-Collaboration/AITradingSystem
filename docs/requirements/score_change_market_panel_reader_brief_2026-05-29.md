# REPORT-055: Score Change Attribution & Market Panel for Reader Brief

最后更新：2026-05-29

## 背景

`REPORT-054` 已把 Reader Brief 做成更清晰的每日阅读入口，但仍有两个读者高价值上下文不完整：

- 今日 score / position / binding gate 相对上一期为什么变化。
- 当日 benchmark、AI sector、risk 和 liquidity 代理实际发生了什么。

本任务只生成和消费只读报告 artifact，不修改 production weights，不生成交易订单，不改变 Reader Brief 的只读边界。

## 范围

1. 完成 `aits reports score-change-attribution --date YYYY-MM-DD` 和 `--latest` 兼容入口，输出 `outputs/reports/score_change_attribution_YYYY-MM-DD.json` / `.md`。
2. 新增 `aits reports market-panel --date YYYY-MM-DD` 和 `--latest`，输出 `outputs/reports/market_panel_YYYY-MM-DD.json` / `.md`。
3. Reader Brief 消费 `score_change_attribution_YYYY-MM-DD.json` 和 `market_panel_YYYY-MM-DD.json`。
4. 所有新增 artifact 固定 `production_effect=none`，缺失价格数据时输出 `MISSING_MARKET_PRICE_DATA`，不得编造市场涨跌。

## 设计边界

- Score change attribution 只比较既有 decision snapshot，不重算 score、weight、gate 或 position。
- Market panel 只读取缓存 `prices_daily.csv` / `rates_daily.csv`，并披露 data quality 状态、source artifact 和限制。
- 缓存数据存在时必须走 `validate_data_cache` 同一质量门禁路径；质量失败时停止生成非降级市场解读。
- Reader Brief 只读消费已生成 artifact，不运行上游 market panel、score change attribution、scoring、backtest、shadow、SEC PIT、weight 或 broker 任务。

## 验收标准

- Score change attribution JSON 包含 current/previous date、overall score、final position、binding gate、component score/contribution deltas、gate state changes、manual review/data quality delta、top positive/negative drivers 和 production_effect。
- Score change attribution Markdown 用自然语言说明主要 component、score vs gate 变化、最终仓位、data quality/manual review 限制。
- previous snapshot 缺失时输出可审计降级状态，不补造归因。
- Market panel JSON 覆盖 benchmark_proxy、ai_sector_proxy、risk_proxy、liquidity_proxy，含 last_price、return_1d、return_5d、return_20d、trend_label、risk_interpretation、data_status、source_artifact、production_effect。
- 价格或利率输入缺失时输出 `MISSING_MARKET_PRICE_DATA`，不编造 market movement。
- Reader Brief 的 `major_score_change` 在归因存在时不再显示 `MISSING`，Market Situation 不再只显示 `MISSING_PRICE_PANEL`，Executive Summary 增加一句市场变化摘要，Missing Artifact Impact 不再把已生成的两个 artifact 列为 IMPORTANT。
- 专项测试覆盖 JSON schema、Markdown、缺上一快照降级、market panel schema、market panel 缺数据降级、Reader Brief 消费两类 artifact、Reader Brief 只读边界和 `production_effect=none`。

## 进展

- 2026-05-29：新增并进入 `IN_PROGRESS`，先补齐任务登记和需求拆解，再实现 artifact 与 Reader Brief 集成。
- 2026-05-29：实现完成并进入 `VALIDATING`。已补齐 score change attribution 读者字段和 `--date` / `--latest`，新增 `market_panel` builder/CLI/Markdown/JSON，Reader Brief 只读消费 market panel 与 score attribution，registry、artifact catalog、system flow 和专项测试已同步；market panel CLI 先运行 data quality gate，门禁失败时写出不含未验证涨跌的 `MISSING_MARKET_PRICE_DATA` 降级 artifact 并停止下游。
