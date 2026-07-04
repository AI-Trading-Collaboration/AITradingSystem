# ENG-VAL-001 Validation Runtime Profiling and Xdist Benchmark
最后更新：2026-07-05

## 背景

TRADING-2345 收尾复盘显示，可记录命令耗时主要集中在验证门禁：

- full validation 约 16m50s，占已记录命令耗时约 80%；
- contract-validation 约 3m23s，占约 16%；
- 当前 `outputs/validation_runtime/*/test_runtime_reader_brief.md` 只记录总耗时，不保留 pytest `--durations` 慢项；
- `scripts/run_validation_tier.py` 已支持 `--workers` 和 `--dist`，但缺少可审计的多分布策略 benchmark 摘要入口。

## 目标

在不降低验证覆盖、不改变默认验证策略、不改变投资研究逻辑的前提下，补齐 validation runtime profiling 能力：

1. runtime artifact 保留 pytest 输出日志；
2. summary JSON 和 Reader Brief 记录 slow-duration 明细；
3. CLI 支持显式 benchmark 多个 xdist distribution / worker 组合；
4. benchmark 结果写入可审计 summary，供后续决定是否调整默认 `--dist loadfile`。

## 安全边界

- 不改变默认 `DEFAULT_WORKERS=16` 或 `DEFAULT_DIST=loadfile`；
- 不减少任何 validation tier 覆盖；
- 不改变 pytest pass/fail 判定；
- 不修改生产配置、策略逻辑、报告结论、market data cache、broker/order 或 paper-shadow 状态；
- benchmark 模式只在显式传入 `--benchmark-dist` 或 `--benchmark-worker` 时运行。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|登记任务与需求|DONE|`docs/task_register.md` 与本需求文档记录范围、边界和验收标准|
|保留 pytest 输出|DONE|`--write-runtime-artifact` 时写出 `pytest_output.log`，summary 不内嵌完整日志|
|解析 slow durations|DONE|summary JSON 和 Reader Brief 包含 slow-duration 明细、数量、总秒数和最慢 nodeid|
|benchmark 入口|DONE|新增显式 benchmark 参数，print-only 可审计计划，真实运行逐变体记录 status / elapsed / slow durations|
|验证与归档|DONE|focused tests、Ruff、compileall、docs freshness、task-register consistency、contract-validation 或等价相关门禁通过|

## 进展记录

- 2026-07-05：根据 TRADING-2345 耗时复盘新增并进入 `IN_PROGRESS`；本任务只增强 validation runtime profiling 和 benchmark 入口，不改变默认验证策略或投资系统行为。
- 2026-07-05：实现完成并归档 `DONE`。新增 `pytest_output.log` runtime artifact、slow-duration parser、Reader Brief 慢项表、summary 慢项字段、`--benchmark-dist` / `--benchmark-worker` 多变体 benchmark 入口和 `validation_benchmark_summary.json`；默认 `DEFAULT_DIST=loadfile` / `DEFAULT_WORKERS=16`、tier 覆盖和 pass/fail 语义保持不变。验证通过 focused parallel pytest 10 passed、Ruff、compileall、benchmark print-only artifact smoke、docs freshness 525 docs PASS、documentation contract 1242 reports PASS、task-register consistency run/validate PASS、contract-validation 196 passed（runtime artifact=`outputs/validation_runtime/contract-validation_20260704T173321Z/test_runtime_summary.json`）和 `git diff --check`。本批未跑 full validation，原因是改动集中在 validation runner 和对应 focused tests，且 full 门禁本身是被优化观察对象；后续默认策略切换必须另开任务并用 benchmark evidence 支撑。
