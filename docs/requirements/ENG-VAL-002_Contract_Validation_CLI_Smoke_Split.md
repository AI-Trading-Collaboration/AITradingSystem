# ENG-VAL-002 Contract Validation CLI Smoke Split
最后更新：2026-07-05

## 背景

ENG-VAL-001 新增 runtime profiling 后，`contract-validation` tier 暴露出单个长尾：

- `tests/test_controlled_strategy_batch.py::test_controlled_strategy_batch_cli_smoke`
  call duration 约 `181.70s`；
- 该测试在同一个 test function 内串行执行完整 controlled strategy / tail-risk
  CLI artifact chain；
- 当前 tier 默认 `--dist loadfile`，同一文件由单个 worker 执行，因此一个超长
  test 会拖慢整个 tier tail latency。

## 目标

在不降低 builder / schema / safety boundary 覆盖、不改变投资研究逻辑、不改变
validation tier 默认策略的前提下，降低 `contract-validation` 长尾耗时：

1. 保留一个真实 CLI end-to-end smoke，覆盖 controlled strategy batch review 的核心链路；
2. 对长尾 command surface 使用 Typer command-tree introspection，确认 wiring 可达；
3. 继续由已拆分的 focused builder tests 覆盖各 artifact 的 schema、safety 和行为；
4. 用 focused pytest 与 `contract-validation --write-runtime-artifact` 记录优化后 runtime。

## 安全边界

- 不修改策略、评分、threshold、position cap、risk multiplier、data quality gate 或报告结论；
- 不修改 cached market / macro / research data；
- 不改变 `scripts/run_validation_tier.py` 默认 `DEFAULT_DIST=loadfile` / `DEFAULT_WORKERS=16`；
- 不减少 `fast-unit` 或 `contract-validation` tier path 覆盖；
- 不跳过 controlled strategy / tail-risk focused builder tests；
- 不生成 paper-shadow、production、official target weight、broker/order 或 runtime mutation。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|登记任务与需求|DONE|`docs/task_register.md` 与本需求文档记录范围、边界和验收标准|
|拆分 CLI smoke 执行策略|DONE|核心 batch-review 链路真实运行；长尾 command surface 通过 Typer command-tree registration smoke 覆盖|
|保持 focused 行为覆盖|DONE|现有 value surface、regime/horizon、tail-risk policy、independent governance 和 candidate batch tests 继续在 tier 内|
|验证 runtime 收益|DONE|focused pytest、Ruff、compileall、contract-validation runtime artifact 和 docs/task gates 通过|
|归档|DONE|任务移动到 completed，并记录最终 runtime 对比|

## 进展记录

- 2026-07-05：根据 ENG-VAL-001 slow-duration evidence 新增并进入 `IN_PROGRESS`。本任务只优化测试组织方式；不改变默认 validation runner 策略、tier path 覆盖、投资系统行为或生产边界。
- 2026-07-05：实现完成并归档 `DONE`。`test_controlled_strategy_batch_cli_smoke`
  保留 5 条核心 batch-review CLI 真实执行，其余 controlled strategy / tail-risk command
  surface 使用 Typer command-tree introspection 验证注册；focused parallel pytest
  `3 passed in 26.12s`，该单测 call duration 从首次 `--help` 尝试的 `171.22s`
  和 ENG-VAL-001 artifact 的 `181.70s` 降到 `20.45s`。完整
  `contract-validation` 通过 `196 passed`，runtime artifact
  `outputs/validation_runtime/contract-validation_20260704T175624Z/test_runtime_summary.json`；
  在完整 tier 中该单测为 `24.79s`，已不再是最慢 nodeid，当前最慢项转为
  `test_data_foundation_cli_smoke` `64.67s` 和 docs contract `52.75s`。本轮完整
  tier elapsed=`213.73s`，高于 ENG-VAL-001 的 `203.44s`，因此结论限定为
  单测级 tail latency 明显下降，tier 总耗时仍需后续针对 data foundation / docs
  contract / tail-risk fixtures 继续优化。
