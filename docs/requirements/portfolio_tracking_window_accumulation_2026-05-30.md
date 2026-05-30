# TRADING-058A Tracking Window Accumulation & Scheduled Review

## 背景

TRADING-058 已完成 Shadow Candidate Performance Review 的工程闭环，但真实
latest review 只有 1 个 tracking day：

```text
status=LIMITED
recommendation=needs_more_data
tracking_days=1
```

该行为正确，系统不应基于单日 tracking 结果形成表现结论。TRADING-058A 的目标是
把“为什么还不能 DONE”转为可审计、可展示、可由每日 review 持续推进的 tracking
window 状态。

## 范围

1. 新增 tracking window policy 配置，定义 short-window 和 extended review 样本门槛。
2. 在 portfolio tracking review artifact 中输出 `tracking_window` 阶段、剩余天数、
   结论许可和 done condition。
3. `tracking_days < 5` 时强制 `needs_more_data`，不得输出正负 performance 结论。
4. `tracking_days >= 5` 后进入 short-window review。
5. `tracking_days >= 20` 后进入 extended review ready。
6. Dashboard、Reader Brief、Markdown report 和 shadow backtest dry-run 展示同一
   tracking window 进度。
7. 新增 `aits portfolio tracking-window-status --latest`，并增强
   `aits portfolio review-tracking --latest --show-window-progress`。
8. 更新 daily scheduled review 链路登记，使 candidate tracking review 可被每日
   scheduler 触达并在 Reader Brief 前生成。

## 非目标和安全边界

- 不修改 `config/parameters/production/current.yaml`。
- 不启用 candidate profile。
- 不解除 production promotion 禁止。
- 不自动 promotion。
- 不降低 data quality gate 或 freshness gate。
- 不伪造、回填或模拟 tracking days。
- 不把 `needs_more_data` 当作失败；它是 `VALIDATING` 状态下的正常结果。

所有输出必须保持：

```text
production_effect=none
manual_review_required=true
auto_promotion=false
```

## Tracking Window 阶段

|Stage|Tracking days|允许行为|
|---|---:|---|
|`initial_observation`|1-4|只能输出 `needs_more_data`|
|`short_window_review`|5-19|允许短窗口表现判断，但不允许 extended review 结论|
|`extended_review_ready`|20+|允许 extended review ready 结论，但不代表 production approval|

## TRADING-058 状态规则

TRADING-058 继续保持 `VALIDATING`，直到最小真实 tracking window 满足：

```text
min_tracking_days >= 5
review_artifact_exists=true
recommendation_not_needs_more_data=true
data_gate=OK
freshness_status=OK
production_effect=none
```

达到 5 个有效 tracking days 后，可进入 `BASELINE_DONE` 或 short-window review。
达到 20 个有效 tracking days 后，才可进入 extended review / DONE 判断。任何状态迁移
仍需同步更新 `docs/task_register.md`。

## 阶段拆解

### 阶段 1：Policy 和核心 payload

- 新增 `config/portfolio/portfolio_tracking_review_windows.yaml`。
- portfolio tracking review payload 新增阶段、剩余天数、结论许可和 done condition。
- validation 检查 stage、safety fields 和 `tracking_days < 5` 的
  `needs_more_data` 约束。

### 阶段 2：只读展示和下游引用

- CLI 增加 `--show-window-progress` 和 `tracking-window-status`。
- Markdown report 增加 `Tracking Window Progress`。
- Dashboard Portfolio Tracking Review 卡片展示 progress。
- Reader Brief 明确剩余天数和最小 review window。
- Shadow backtest dry-run reason 引用最小窗口不足或 short-window 状态。

### 阶段 3：Scheduled review 和文档

- daily scheduled task 登记加入 freshness、candidate tracking、tracking review 和
  portfolio tracking report alias。
- 更新 `docs/system_flow.md`、相关 runbook 和 artifact catalog。

### 阶段 4：验证

- 新增/增强专项测试覆盖 1、4、5、19、20 tracking days，Dashboard、Reader Brief、
  shadow backtest reason、安全字段和 production/current.yaml unchanged。
- 运行：
  - `python -m pytest -q`
  - `python -m ruff check scripts src tests`
  - `python -m compileall src scripts`
  - `git diff --check`

## 状态记录

- 2026-05-30：新增并进入 `IN_PROGRESS`。原因：TRADING-058 latest 真实状态为
  `tracking_days=1` / `needs_more_data`，需要把 tracking window 累积、阶段判断、
  每日 review 和读者展示正式纳入系统，而不是提前关闭 TRADING-058。
- 2026-05-30：从 `IN_PROGRESS` 改为 `VALIDATING`。原因：已完成 tracking window
  policy、payload stage/days-until、CLI、Dashboard、Reader Brief、shadow backtest
  reason、daily scheduled review 登记、artifact catalog 和 system flow 更新；真实
  latest 仍为 `tracking_days=1` / `stage=initial_observation` /
  `recommendation=needs_more_data`，TRADING-058 继续等待最小 5 个有效 tracking days。

## 验证记录

- `python -m ai_trading_system.cli portfolio review-tracking --latest --show-window-progress`：
  通过，输出 `stage=initial_observation`、`days_until_short_review=4`、
  `production_effect=none`。
- `python -m ai_trading_system.cli portfolio tracking-window-status --latest`：通过。
- `python -m ai_trading_system.cli reports portfolio-tracking-review --latest`：通过。
- `python -m ai_trading_system.cli reports reader-brief --latest`：通过，Reader Brief 明确
  only 1 tracking day / at least 5 valid tracking days。
- `python -m ai_trading_system.cli parameters shadow-backtest --latest --dry-run`：通过，
  promotion 仍为 `rejected`，reason 明确最小 tracking window 不足。
- `python -m pytest -q`：1543 passed，330 warnings。
- `python -m ruff check scripts src tests`：通过。
- `python -m compileall src scripts`：通过。
- `git diff --check`：通过。
- `config/parameters/production/current.yaml`：未修改。
