# TRADING-865～878 Simple Baseline Portfolio Control
最后更新：2026-06-23

## 背景

Tail-risk fallback governance closeout 后，当前研究主线从复杂 fallback 修复转向
QQQ / TQQQ / SGOV 的简单强基准和动态仓位控制。该批任务只建立
research-only / observe-only 的可审计研究闭环，不进入 paper-shadow，不触发
production 或 broker。

默认解释窗口使用 `ai_after_chatgpt` regime：anchor event 为 2022-11-30
ChatGPT 公开发布，默认回测开始日为 2022-12-01。更早数据只可用于 warm-up、
压力测试或 regime 对比，不能作为默认 AI-cycle 结论窗口。

## 安全边界

所有 TRADING-865～878 artifact 必须固定：

- `production_effect=none`
- `broker_action=none`
- `promotion_allowed=false`
- `paper_shadow_allowed=false`
- `production_allowed=false`
- `manual_review_required=true`
- `research_only=true`
- `observe_only=true`

Reader Brief 只能展示 research-only / observe-only 摘要，不得输出买入、卖出、
真实订单、真实账户调整或 paper-shadow activation。

## 任务拆解

|任务|范围|状态|
|---|---|---|
|TRADING-865|建立 simple baseline strategy registry 和 registry review CLI|IN_PROGRESS|
|TRADING-866|QQQ / SGOV static、trend、vol、equal-risk baseline backtest|IN_PROGRESS|
|TRADING-867|TQQQ / SGOV risk-controlled baseline backtest|IN_PROGRESS|
|TRADING-868|趋势 / 波动 / 回撤动态仓位 policy search|IN_PROGRESS|
|TRADING-869|统一 dominance ranking 和 Pareto frontier|IN_PROGRESS|
|TRADING-870|simple baseline PIT boundary audit|IN_PROGRESS|
|TRADING-871|transaction cost and rebalance sensitivity|IN_PROGRESS|
|TRADING-872|regime-stratified simple baseline review|IN_PROGRESS|
|TRADING-873|simple baseline forward aging tracker|IN_PROGRESS|
|TRADING-874|paper-shadow readiness review，禁止自动进入 paper-shadow|IN_PROGRESS|
|TRADING-875|Reader Brief portfolio control safety summary|IN_PROGRESS|
|TRADING-876|hypothetical portfolio dry-run mapper，禁止 broker/order|IN_PROGRESS|
|TRADING-877|simple baseline master review|IN_PROGRESS|
|TRADING-878|LEAPS / Wheel next-stage options gate，只建 gate 不启动期权研究|IN_PROGRESS|

## 实施顺序

1. 第一批：TRADING-865、866、867、870。先建立 registry、QQQ/SGOV 和
   TQQQ/SGOV 强基准，并审计 PIT 边界。
2. 第二批：TRADING-868、869、871、872。再比较动态仓位、dominance、成本敏感性
   和 regime robustness。
3. 第三批：TRADING-873、874、875、876。纳入 forward aging、paper-shadow review
   条件、安全摘要和 dry-run mapping。
4. 第四批：TRADING-877、878。输出主审查和 LEAPS / Wheel 下一阶段 gate。

## 数据质量和 PIT 要求

产生 backtest、ranking、cost sensitivity、regime review、forward aging 或 dry-run
映射的命令必须先调用 `validate_data_cache` 同一路质量门禁，并在 JSON/Markdown
中披露 `data_quality_status`。若质量门禁失败，命令必须输出 blocked artifact，
不得继续计算收益、回撤或 readiness 结论。

所有 signal / allocation 规则必须只使用 decision_time 可见字段。趋势、波动、
回撤和再平衡信号在 t 日执行时只能使用 t-1 或更早价格信息；future return、
future drawdown、future realized volatility、tail-risk label、fallback trigger 或
任何非 decision_time 字段均不得作为策略输入。

## 验收标准

- 新增 `config/research/simple_baseline_strategy_registry.yaml`，含策略定义、
  policy metadata、heuristic rationale、review condition 和 safety boundary。
- 新增 `aits research strategies ...` CLI 覆盖 TRADING-865～878。
- 每个 CLI 写出 JSON 和 Markdown artifact；TRADING-877 额外写出
  `docs/research/simple_baseline_portfolio_control_master_review.md`。
- `config/report_registry.yaml` 登记所有新增 report，除 TRADING-875 外
  `required_for_daily_reading=false`。
- `docs/artifact_catalog.md` 和 `docs/system_flow.md` 说明新数据流、输入、输出、
  下游用途和 no-production 边界。
- Reader Brief 增加 portfolio control research status，只读展示安全字段。
- Focused pytest、CLI smoke、report registry validation、contract validation、
  Ruff、compileall 和 `git diff --check` 通过或明确记录阻塞原因。

## 当前开放问题

- 真实本地价格缓存是否已经包含 QQQ、TQQQ、SGOV 全量日线和可用的宏观 rates；
  缺失时相关 backtest/forward aging 命令必须 fail closed。
- TQQQ-heavy 策略仅可作为 research-only 风险识别对象；即使历史收益更高，也必须
  单独披露 leverage decay、path dependency、最大月/季亏损和 recovery days。
- LEAPS / Wheel 在本批只建立 gate。进入期权研究前必须先有 ETF 权重主线的稳定
  paper-shadow candidate、足够 forward aging 样本和明确 option chain historical
  data contract。
