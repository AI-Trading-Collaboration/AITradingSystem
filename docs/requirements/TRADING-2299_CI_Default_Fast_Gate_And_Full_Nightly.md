# TRADING-2299: CI Default Fast Gate And Full Nightly

最后更新：2026-06-29

## 背景

GitHub Actions CI 当前每次 push / pull request 都运行
`python scripts/run_validation_tier.py full`，即完整 `tests` 目录。近期 main 上连续小提交导致
多个 CI run 各自运行约一小时，反馈周期过长，并且旧 run 不会被新 push 自动取消。

`full` tier 在 validation runner 中定义为 broad changes final validation，不适合作为每个
小提交的默认反馈门。默认 CI 应快速发现 lint、docs freshness 和核心 contract / registry
问题，同时保留 full gate 的夜间和人工触发入口。

## 目标

- push / pull request 默认运行 `fast-unit` validation tier。
- `full` validation tier 保留为 nightly scheduled run 和 manual workflow dispatch option。
- 同一 workflow/ref 的 push 或 pull request 新 run 自动取消旧 run，避免一串过期 full/fast run
  占用 runner。
- pytest failure annotation 逻辑保持可用。

## 非目标

- 不降低本地变更验证纪律；大范围改动仍应按影响面手动运行 `full` 或更具体 tier。
- 不改变 runtime data quality gate、scoring、backtest、paper-shadow、production 或 broker path。
- 不修改 validation tier runner 的 suite 定义。

## 验收标准

- `.github/workflows/ci.yml` 默认 `VALIDATION_TIER=fast-unit`。
- workflow dispatch 支持 `fast-unit`、`contract-validation` 和 `full`。
- schedule 自动选择 `full`。
- push / pull request 配置 `concurrency.cancel-in-progress`。
- focused tests、Ruff、docs freshness 和 `git diff --check` 通过。

## 进展记录

- 2026-06-29: 新增并进入 `IN_PROGRESS`；owner 要求缩短 CI 默认反馈时间。
- 2026-06-29: 实现完成并转入 `VALIDATING`；本地验证通过 focused parallel pytest
  10 passed、Ruff、docs freshness、`git diff --check` 和 `fast-unit` tier
  194 passed（runtime artifact=`outputs/validation_runtime/fast-unit_20260629T015021Z/test_runtime_summary.json`）；
  等待 GitHub Actions 新 run 结果。
- 2026-06-29: GitHub Actions run #712 失败于 clean-cache 环境下的
  `test_source_qualification_remediation_contract` 固定本机 cache count 断言；测试合同改为校验
  updated acceptance summary 与 acceptance report 自洽，并继续要求 diagnostic / blocked
  fail-closed source counts 覆盖至少 5 个 qualification items，不放宽 data-quality、scoring、
  backtest、report 或 production gate。
- 2026-06-29: 修复后验证通过 focused parallel pytest 11 passed、clean worktree targeted
  pytest 1 passed、Ruff、docs freshness、`git diff --check` 和 `fast-unit` tier
  194 passed（runtime artifact=`outputs/validation_runtime/fast-unit_20260629T021606Z/test_runtime_summary.json`）。
