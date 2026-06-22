# TRADING-894: Daily Incremental Refactor Simple Baseline CLI Boundary

关联任务：`TRADING-894_DAILY_INCREMENTAL_REFACTOR_SIMPLE_BASELINE_CLI_BOUNDARY`

## 背景

2026-06-23 每日增量重构巡检发现，自最近一次重构基线 `5fe628e5` 之后，
`src/ai_trading_system/cli_commands/research.py` 从 research foundation CLI 拆分后的
状态重新增长到 8000 行以上。新增的 TRADING-865～893 simple baseline / QQQ-TQQQ-SGOV
候选验证命令都挂在 `aits research strategies ...` 下，但它们的底层实现已经位于
`simple_baseline_portfolio_control.py` 和 `simple_baseline_candidate_validation.py`，CLI
wrapper 仍集中在 `research.py` 中，增加后续维护和审计定位成本。

## 目标

将 simple baseline strategy CLI wrapper 从 `research.py` 拆到专用
`src/ai_trading_system/cli_commands/research_simple_baselines.py`，并由 `research.py`
继续注册到同一个 `strategies_app`。

外部命令路径、参数、artifact 输出、安全字段和退出语义必须保持兼容：

- `aits research strategies simple-baseline-registry-review`
- `aits research strategies qqq-sgov-baseline-backtest`
- `aits research strategies tqqq-sgov-risk-controlled-baseline`
- `aits research strategies trend-vol-allocation-policy-search`
- `aits research strategies simple-baseline-dominance-ranking`
- `aits research strategies simple-baseline-pit-boundary-audit`
- `aits research strategies simple-baseline-cost-sensitivity`
- `aits research strategies simple-baseline-regime-review`
- `aits research strategies simple-baseline-forward-aging-tracker`
- `aits research strategies simple-baseline-paper-shadow-readiness`
- `aits research strategies daily-reader-portfolio-control-safety-summary`
- `aits research strategies simple-baseline-portfolio-dry-run-mapper`
- `aits research strategies simple-baseline-master-review`
- `aits research strategies options-next-stage-gate`
- `aits research strategies equal-risk-qqq-sgov-deep-dive`
- `aits research strategies simple-baseline-period-split-validation`
- `aits research strategies simple-baseline-drawdown-episode-review`
- `aits research strategies dynamic-vs-static-edge-significance-review`
- `aits research strategies tqqq-heavy-pause-rationale-report`
- `aits research strategies simple-baseline-watchlist-owner-decision`

## 阶段

1. 新增专用 CLI 模块，迁移 simple baseline imports、command wrappers 和输出 helper。
2. 在 `research.py` 中移除 simple baseline wrapper 代码，只保留注册调用。
3. 更新 `docs/system_flow.md`，明确该命令族由专用 CLI 模块承载但命令面不变。
4. 更新 `docs/refactor_log.md` 记录本轮评估范围、变更理由、验证结果和提交 SHA。
5. 运行 focused CLI/tests、格式/静态/文档一致性检查和 `git diff --check`。

## 安全边界

本任务只做 CLI wrapper 模块边界整理：

- 不改变 simple baseline 计算逻辑、数据质量 gate、cache schema、report schema、policy
  threshold、score band、promotion gate、position cap 或 backtest acceptance rule。
- 不运行 broker、order 或 trading action。
- 不写 production weights、active shadow weights、paper account 或 production state。
- 涉及 cached price/rate 的既有 simple baseline 命令仍由底层函数调用同源
  `validate_data_cache` 并在 artifact 中披露 data quality；本重构本身不生成新的
  cached-data dependent output。

## 验收标准

- `research.py` 中 simple baseline wrapper 被移出，`research_simple_baselines.py`
  承载注册逻辑。
- 上述 `aits research strategies ...` 命令仍可被 Typer help/CLI smoke 发现。
- `tests/test_simple_baseline_portfolio_control.py` 和相关 CLI contract tests 通过。
- Ruff、compileall、documentation/task-register focused tests 和 `git diff --check` 通过。
- `docs/refactor_log.md` 记录本轮重构并在提交后回填 SHA。

## 进展记录

- 2026-06-23：每日增量重构巡检新增，状态 `IN_PROGRESS`；最近一次合格重构基线
  为 `5fe628e5`，本轮选择 simple baseline CLI boundary 作为低风险结构整理切片。
- 2026-06-23：实现完成并归档为 `DONE`。新增
  `src/ai_trading_system/cli_commands/research_simple_baselines.py`，迁移 20 个
  `aits research strategies ...` simple baseline command wrappers；`research.py`
  只保留 `strategies_app` 注册调用。外部命令、参数、artifact path、输出 safety
  fields 和退出语义保持兼容。验证通过 focused parallel pytest 13 passed、
  `contract-validation` 193 passed（runtime artifact
  `outputs/validation_runtime/contract-validation_20260622T230834Z/test_runtime_summary.json`）、
  Ruff、Black check、compileall、CLI help smoke 和 `git diff --check`。本任务未生成新的
  cached-data dependent output，因此未额外运行 `aits validate-data`；底层 simple baseline
  数据依赖命令仍按既有实现调用同源 `validate_data_cache`。
