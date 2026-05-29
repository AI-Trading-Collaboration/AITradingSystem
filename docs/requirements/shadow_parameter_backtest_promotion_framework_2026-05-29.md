# Shadow Parameter Backtest & Promotion Framework

状态：BASELINE_DONE
任务：CALIBRATION-020
最后更新：2026-05-29

## 背景

当前系统已有 daily scheduler、Reader Brief、Market Panel、Score Change
Attribution、Research Governance Summary、Daily Weight Adjustment Summary、
scheduler template validation、任务注册表、artifact catalog、system flow 和 runbook。
现有 shadow parameter search / promotion contract 主要围绕短样本搜索和 forward
shadow 生命周期，仍缺一条独立的 production baseline vs shadow candidate
walk-forward 回测、参数变化归因、晋升资格判断和只读报告闭环。

本任务新增 Shadow Parameter Backtest & Promotion Framework。它只能生成
observe-only 产物，不得自动修改 production 参数、production weights、hard gates、
approved overlay、broker order、prediction ledger 或交易动作。

## 目标

1. 支持读取 baseline production 参数快照。
2. 支持按受控搜索空间生成 shadow 参数候选。
3. 支持 walk-forward 日频回测和 baseline/candidate 对比。
4. 支持参数变化归因、过拟合风险提示和人工复核 checklist。
5. 支持 promotion decision：`rejected`、`watch`、`candidate`、
   `manual_review_required`。
6. 输出 JSON / Markdown，并接入 dashboard 与 Reader Brief 的只读摘要。
7. 默认固定：
   - `production_effect: none`
   - `manual_review_required: true`
   - `auto_promotion: false`
   - `observe_only: true`

## 非目标

- 不接真实交易执行。
- 不自动修改 `config/parameters/production/current.yaml`。
- 不直接下单。
- 不引入复杂 ML 模型。
- 不做期权、高频或分钟级回测。
- 不声明严格 Point-in-Time 新闻或完整 PIT fundamentals 有效性。
- 不让 LLM 生成或覆盖 production 参数。

## 阶段拆解

### Phase 1: Schema & Config

- 新增 `config/parameters/production/current.yaml`。
- 新增 `config/parameters/shadow/shadow_backtest.yaml`。
- 新增 `config/parameters/promotion/promotion_rules.yaml`。
- 实现 production/shadow 参数 schema、loader、weight sum validation 和 diff。
- 验收：parameter schema、weight sum、parameter diff 测试通过。

### Phase 2: Walk-forward Backtest Skeleton

- 实现 walk-forward window generator。
- 实现日频 portfolio simulator、transaction cost 和基础指标。
- 数据不足输出 `INSUFFICIENT_DATA`，不得生成误导性 candidate。
- 验收：window split、transaction cost、insufficient data 测试通过。

### Phase 3: Shadow Parameter Generator

- 实现 bounded grid search。
- 遵守 weight sum、单项上下限、相对 baseline 变化和 L1 guardrail。
- hard gates 只评估，不自动优化阈值。
- 验收：候选生成、guardrail、overfitting warning 测试通过。

### Phase 4: Promotion Rules

- 实现 promotion criteria 和 hard rejection rules。
- 数据质量 `INSUFFICIENT_DATA` / `FAILED` 强制 rejected。
- 所有 major changes 必须有 reason。
- 验收：promotion rule、hard rejection、多窗口稳定性测试通过。

### Phase 5: Reports & Dashboard

- 输出：
  - `artifacts/shadow_backtest/YYYY-MM-DD/shadow_backtest_summary.json`
  - `artifacts/shadow_backtest/YYYY-MM-DD/shadow_backtest_summary.md`
  - `artifacts/parameter_promotion/YYYY-MM-DD/parameter_promotion_decision.json`
  - `artifacts/parameter_promotion/YYYY-MM-DD/parameter_promotion_decision.md`
- 新增 CLI：
  - `aits parameters shadow-backtest --latest`
  - `aits parameters shadow-backtest --date YYYY-MM-DD`
  - `aits parameters shadow-backtest --config config/parameters/shadow/shadow_backtest.yaml`
  - `aits parameters validate-shadow-backtest --latest`
  - `aits reports shadow-parameter-backtest --latest`
  - `aits reports parameter-promotion --latest`
- Dashboard 新增只读 `Shadow Parameter Backtest` 卡片。
- Reader Brief 新增简短 `Parameter Shadow Review` 摘要。
- 同步更新 artifact catalog、system flow、runbook 和 report registry。

## 配置与阈值治理

本任务涉及投资解释阈值，必须在配置中携带 owner、status、rationale、
intended effect、validation evidence 或 planned validation、review/expiry condition。
默认 pilot baseline 包括：

- walk-forward 窗口：756 / 126 / 63 / min 1008 days。
- transaction cost：commission 1 bps、slippage 5 bps、fx 0 bps、tax ignored for v0.1。
- parameter change guardrails：单项 0.10、L1 0.30。
- promotion criteria：return、Sharpe、drawdown、turnover、recent period、stability 和 explainability。
- v0.1 portfolio simulator 的 signal-score 到 risk-budget 映射是临时 pilot
  baseline，作为 `V0_1_RISK_BUDGET_BY_SCORE` 命名常量保留相邻代码注释；
  后续应迁移到 reviewed policy/config 后再用于更高可信度结论。

## 数据质量与 PIT 边界

- 依赖 cached market/macro data 的执行必须先运行 `aits validate-data` 或同一路径。
- 输出必须显示 data quality status 和 quality report path。
- `INSUFFICIENT_DATA` 或 `FAILED` 时：
  - `promotion_status: rejected`
  - `production_effect: none`
  - 不生成误导性 candidate。
- PIT 状态必须披露：
  - price data：OK 或 LIMITED。
  - fundamental data：LIMITED。
  - news data：NOT_AVAILABLE。
  - macro data：LIMITED。

## 验收标准

- `aits parameters shadow-backtest --latest` 可以运行。
- `aits reports shadow-parameter-backtest --latest` 可以读取最新产物。
- JSON/Markdown 明确展示 baseline、candidate、参数变化、walk-forward 结果、
  风险指标、晋升判断和人工复核要求。
- Dashboard 展示 `Shadow Parameter Backtest` 只读卡片。
- Reader Brief 展示简短 Parameter Shadow Review。
- 默认不修改 production 参数。
- 数据不足时不得输出误导性 candidate。
- 目标 pytest、ruff、black check 通过或明确记录既有阻断原因。

## 进展记录

- 2026-05-29：新增任务和需求文档，进入 IN_PROGRESS。实现范围为
  v0.1 observe-only 基础设施，优先满足 schema、walk-forward skeleton、
  bounded candidate、promotion decision、report / CLI / dashboard / Reader Brief
  接线和目标测试。
- 2026-05-29：完成 v0.1 baseline。新增源码模块、配置、CLI、JSON/Markdown
  report、Dashboard 只读卡片、Reader Brief 摘要、artifact catalog、system flow、
  runbook 和测试。验证通过目标 pytest、受影响 dashboard/Reader Brief/CLI
  dispatcher tests、ruff、black check；dry-run 样例在本地缓存质量门禁失败时输出
  `DEGRADED` / `rejected`，未修改 production 参数。

## 剩余边界

- 当前 v0.1 使用 price-derived feature proxies，不声明严格 production-grade PIT
  fundamentals/news 有效性。
- 当前 hard gates 只评估不调参，人工 promotion workflow 尚未实现。
- 长期 shadow parameter leaderboard、PIT 数据升级、factor-level attribution 和
  tax-aware simulation 作为后续任务处理。
