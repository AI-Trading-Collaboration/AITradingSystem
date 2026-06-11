# TRADING-189 to 198 Defensive Hypothesis Review and Forward Evidence Automation

最后更新：2026-06-11

## 背景

TRADING-184～188 已完成 pressure regime sample expansion 和 defensive rule validation
的基础闭环。当前真实结论是：`defensive_limited_adjustment` 只有
`BACKTEST_SIMULATION` 研究证据，`FORWARD_OUTCOME` 与 `HISTORICAL_REPLAY` pressure
evidence 仍不足；因此它必须保持 `RESEARCH_ONLY`，不得进入 rule approval、
production、broker action 或 auto policy apply。

本阶段并行推进两条线：

- A 组：基于 simulation pressure evidence 深挖 defensive hypothesis、失败窗口和命名风险。
- B 组：建立 forward/PIT pressure evidence 的 daily、weekly 与 event-driven 收集机制。

## 阶段拆解

### TRADING-189 Simulation Pressure Evidence Deep Dive

新增 `defensive-hypothesis-deep-dive run/report` 与
`validate-defensive-hypothesis-deep-dive`。输出 supporting/contradicting cases、
regime effect matrix、exposure attribution 和中文报告。验收重点是明确
simulation evidence 只能支持研究假设，不能支持 production rule approval。

### TRADING-190 Defensive Label Rename / Classification Review

新增 `defensive-label-review run/report` 与 `validate-defensive-label-review`。
输出 label decision matrix、candidate labels、Reader Brief section。该流程只给出命名
或 warning 建议，不修改配置，`auto_rename=false`。

### TRADING-191 Defensive Variant Failure Case Study

新增 `defensive-failure-study run/report` 与 `validate-defensive-failure-study`。
输出失败窗口排序、failure pattern summary 与 mitigation ideas。所有 mitigation ideas
均为 research ideas，`auto_apply=false`。

### TRADING-192 Defensive Hypothesis Research Note

新增 `defensive-research-note run/report` 与 `validate-defensive-research-note`。
整合 deep dive、label review 和 failure study，输出 research note、summary JSON 与
Reader Brief section，固定 `can_support_rule_approval=false`。

### TRADING-193 Owner Defensive Hypothesis Decision Pack

新增 `defensive-owner-pack run/report` 与 `validate-defensive-owner-pack`。
输出 owner decision options、checklist 和报告。推荐 owner 继续跟踪、保留 warning、
优先收集 forward samples；不得 auto apply、policy change 或 broker action。

### TRADING-194 Forward Pressure Evidence Capture Plan

新增 `config/etf_portfolio/dynamic_v3_rescue/forward_pressure_capture_v1.yaml`、
`forward-pressure-capture plan/report` 与 `validate-forward-pressure-capture`。计划必须
区分 daily、weekly 和 event-driven commands，并固定
`broker_action_allowed=false`、`production_effect=none`、`auto_apply_policy=false`。

### TRADING-195 Daily Pressure Trigger Scanner

新增 `pressure-trigger scan/report` 与 `validate-pressure-trigger`。scanner 使用
QQQ/SMH 的 1d/1w return 或 drawdown 与 capture config 阈值比较，未触发时不运行重流程；
触发时输出 recommended event-driven commands。

### TRADING-196 Event-driven Pressure Capture Workflow

新增 `pressure-capture run/report` 与 `validate-pressure-capture`。`NO_TRIGGER` 默认安全跳过；
`--force` 允许手动生成 workflow，但必须标记 `manual_force=true`。该 workflow 只更新 evidence，
不改 policy，不触发 broker。

### TRADING-197 Forward / PIT Pressure Sample Ledger

新增 `pressure-sample-ledger update/report` 与 `validate-pressure-sample-ledger`。ledger
长期区分 `FORWARD_OUTCOME`、`HISTORICAL_REPLAY`、`BACKTEST_SIMULATION`，只有合格 forward
或 PIT evidence 才能成为 rule approval eligible；simulation 默认
`can_support_rule_approval=false`。

### TRADING-198 Weekly Defensive Evidence Update

新增 `weekly-defensive-evidence run/report` 与 `validate-weekly-defensive-evidence`。
每周汇总新增 pressure samples、forward gap、defensive rule status、owner attention 与
Reader Brief section；默认建议 `continue_tracking`，`policy_change_allowed=false`。

## 依赖与顺序

1. 先实现 artifact workflow 与 focused tests。
2. 使用最新 TRADING-184～188 artifacts 跑通真实链路。
3. 更新 README、operations runbook、system flow、report registry、artifact catalog、
   task register 和 Reader Brief 文档。
4. 运行 focused tests、ruff、compileall、git diff check、dynamic-v3 validation 与
   artifact family validation。

## 安全边界

所有新增流程必须固定：

- `auto_apply=false` 或 `auto_apply_policy=false`
- `policy_change_allowed=false`
- `broker_action_allowed=false`
- `broker_action_taken=false`
- `production_effect=none`
- simulation evidence 不得支持 rule approval

## 验收标准

- 附件要求的 10 组 CLI 均可运行。
- 附件列出的新增 artifacts 均可生成并通过对应 validate CLI。
- Reader Brief section 可由 research note、label review 和 weekly defensive evidence 输出。
- `aits etf dynamic-v3-rescue validate` 与
  `aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue` 通过。
- Focused tests、`python -m ruff check src tests`、`python -m compileall -q src tests`、
  `git diff --check` 通过。
- 尽量运行全量 pytest；若超时，记录已完成测试和失败/超时信息。

## 进展记录

- 2026-06-11：新增需求文档并进入实现。初始范围为附件中的 TRADING-189～198，按一个大功能闭环交付；不得用 simulation-only evidence 批准 defensive rule。
- 2026-06-11：实现 10 组 CLI、配置、Reader Brief 聚合、report registry、artifact catalog、system flow、operations runbook 和 focused tests。本地真实链路使用 pressure backfill `af2d09c3b5aabc6e`、defensive comparison `6d3ea9b43618c7db` 生成：deep dive `7dde2defa8472fdd`、label review `839b841071b71eee`、failure study `603f3464fbb75826`、research note `098efbf1be65506c`、owner pack `386d63af79363f22`、capture plan `0e666a62ab48b6c3`、pressure trigger `10f11e44b3a6ac84`、pressure capture `77ce7f2ea4d9cb35`、pressure sample ledger `c252b4af86d5ab46`、weekly defensive evidence `752808d0ef02b3ba`；对应 validate CLI 均为 `PASS`。当前 ledger 为 `forward_samples=0`、`simulation_samples=116`、`progress_to_requirement=0.0`，因此 defensive rule 仍为 `RESEARCH_ONLY`。
- 2026-06-11：验证通过 `python -m ruff check src tests`、`python -m compileall -q src tests`、`git diff --check`、focused pytest 12 passed、`aits etf dynamic-v3-rescue validate`、`aits etf dynamic-v3-rescue artifacts validate --family dynamic_v3_rescue`、`aits reports index --latest`、`aits reports reader-brief --latest` 和 `aits reports validate-reader-brief --latest`。全量 `python -m pytest tests -q` 已尝试，10 分钟超时且未返回失败详情；本阶段不把 full pytest 记为通过。
