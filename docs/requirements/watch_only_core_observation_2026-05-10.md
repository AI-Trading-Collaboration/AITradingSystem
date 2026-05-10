# 核心观察池 Watch-only 分析语义

## 背景

2026-05-10 项目 owner 明确：`MSFT`、`GOOG`、`TSM`、`INTC`、`AMD`、`NVDA` 当前都处于观察阶段，不作为主动交易 thesis 驱动的单票交易对象。系统仍需要结合 AI 产业链趋势、核心 ticker 走势、基本面、估值和风险复核给出观察分析。2026-05-11 核心观察池扩展后，新增代表性 ticker 默认也按 `watch_only` 进入观察分析，不自动要求主动交易 thesis。

当时配置把这 6 个核心 ticker 都标记为 `thesis_required: true`。当 `data/external/trade_theses/` 不存在时，`thesis` 复核产生警告，`score-daily` 的 thesis gate 把仓位上限压到 70%，这会把“没有主动交易 thesis”误解释为“主动交易 thesis 缺失”，不符合当前观察阶段语义。

## 设计决策

- 在 `config/watchlist.yaml` 中显式区分观察池 ticker 的使用阶段。
- `watch_only` ticker 只用于趋势、产业链节点热度/健康度、风险和估值观察分析；它不是主动单票交易候选。
- `watch_only` ticker 可以不要求交易 thesis；缺少 thesis 不应触发 thesis gate。
- `active_trade` ticker 仍沿用严格规则：高风险或极高风险标的必须要求 thesis，且缺少 active thesis 会进入复核警告并约束仓位。
- 产业链趋势分析继续使用核心观察池 ticker 和行业节点映射，保持 `production_effect=none`，不直接改变评分、仓位闸门或执行建议。

## 阶段拆解

1. 配置语义
   - 给 watchlist item 增加 `decision_stage`，枚举值为 `watch_only` 和 `active_trade`。
   - 将当时 6 个核心 ticker 设置为 `watch_only`，并将 `thesis_required` 设置为 `false`。

2. 校验规则
   - `watchlist validate` 对 `watch_only` 高风险 ticker 不再报 `high_risk_without_thesis` 错误。
   - `active_trade` 高风险 ticker 缺少 thesis 仍报错。
   - `thesis validate/review` 在没有任何活跃 ticker 要求 thesis 时，不因 thesis 目录不存在或无文件产生 warning。

3. 报告解释
   - 观察池报告展示 ticker 的使用阶段。
   - 系统流图和架构文档说明 watch-only ticker 仍进入行业趋势分析，但不触发主动交易 thesis gate。

## 验收标准

- 默认 `aits watchlist validate` 对当前核心观察池返回 `PASS`。
- 当时 6 个核心 ticker 不再因为缺少 `data/external/trade_theses/` 让 `aits thesis review` 返回 `PASS_WITH_WARNINGS`。
- `active_trade` 高风险 ticker 如果 `thesis_required=false` 仍会失败，避免放松主动交易纪律。
- 日报仍保留“关注股票趋势分析”和“产业链节点热度与健康度”，用于观察阶段解释行业趋势。
- `docs/system_flow.md` 同步说明 watch-only 与 active-trade thesis gate 的边界。

## 进展记录

- 2026-05-10：新增需求文档；开始实现 watch-only 核心观察池语义。
- 2026-05-10：完成实现。新增 `decision_stage` 配置语义，将当时 6 个核心 ticker 设置为 `watch_only`，观察阶段不再因缺少 `data/external/trade_theses/` 触发 thesis warning 或 thesis gate；主动交易阶段高风险 ticker 仍必须要求 thesis。验证通过 `ruff check src tests`、`pytest -q`、`watchlist validate`、`thesis review` 和 `watchlist validate-lifecycle`。
