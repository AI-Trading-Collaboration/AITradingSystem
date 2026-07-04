# ENG-VAL-003 Data Foundation CLI Smoke Split
最后更新：2026-07-05

## 背景

ENG-VAL-002 解除 controlled strategy CLI smoke 的单测级长尾后，
`contract-validation` runtime artifact 显示新的最慢项为：

- `tests/test_data_foundation_roadmap.py::test_data_foundation_cli_smoke`
  `64.67s`；
- docs contract `52.75s`；
- tail-risk fixture 群约 `18s`～`25s`。

`test_data_foundation_cli_smoke` 在一个 test function 内串行执行 data foundation
各命令，而同文件已有 direct API tests 覆盖 PIT snapshot、asset master、trading
costs、research labels/runs/execution、forward evidence 和 case library 的 schema /
safety / artifact 行为。

## 目标

在不降低 data foundation 行为覆盖、不改变 data quality gate、report contract 或
validation tier 默认策略的前提下，降低 `contract-validation` 新长尾：

1. 保留少量跨 root group 的真实 CLI smoke，覆盖 data / trading-costs / research /
   forward-evidence 的 Typer dispatch 与 artifact 写入；
2. 对其余 data foundation command surface 使用 Typer command-tree introspection
   验证注册；
3. 继续由 direct API tests 覆盖 schema、safety boundary、artifact creation 和
   append-only behavior；
4. 用 focused pytest 与 `contract-validation --write-runtime-artifact` 记录优化后
   runtime，并明确 tier 总耗时是否同步改善。

## 安全边界

- 不修改 data foundation production code、data quality gate、cached market / macro data、
  report registry、artifact schema 或 report interpretation；
- 不改变 `scripts/run_validation_tier.py` 默认 `DEFAULT_DIST=loadfile` /
  `DEFAULT_WORKERS=16`；
- 不减少 `fast-unit` 或 `contract-validation` tier path 覆盖；
- 不跳过 direct API 行为测试；
- 不生成 paper-shadow、production、official target weight、broker/order 或 runtime mutation。

## 实施步骤

|步骤|状态|验收标准|
|---|---|---|
|登记任务与需求|DONE|`docs/task_register.md` 与本需求文档记录范围、边界和验收标准|
|拆分 data foundation CLI smoke|DONE|核心跨 root group CLI 真实运行；其余 command surface 通过 Typer registration smoke 覆盖|
|验证 runtime 收益|DONE|focused pytest、Ruff、compileall、contract-validation runtime artifact 和 docs/task gates 通过|
|归档|DONE|任务移动到 completed，并记录最终 runtime 对比|

## 进展记录

- 2026-07-05：根据 ENG-VAL-002 后的 `contract-validation` slow-duration evidence 新增并进入
  `IN_PROGRESS`。本任务只优化测试组织方式，不改变 data foundation runtime behavior、
  validation runner 默认策略、tier path 覆盖或生产边界。
- 2026-07-05：实现完成并归档 `DONE`。`test_data_foundation_cli_smoke`
  保留 6 条跨 root group 核心 CLI 真实执行，其余 data foundation command surface
  使用 Typer command-tree introspection 验证注册；direct API tests 继续覆盖 schema /
  safety / artifact 行为。Focused parallel pytest `4 passed in 16.35s`，目标单测
  call duration 从 ENG-VAL-002 artifact 的 `64.67s` 降到 `10.59s`。完整
  `contract-validation` 通过 `196 passed`，runtime artifact
  `outputs/validation_runtime/contract-validation_20260704T180921Z/test_runtime_summary.json`；
  tier elapsed 从 `213.73s` 降到 `191.61s`，top20 slow duration total 从 `474.64s`
  降到 `394.89s`。当前最慢 nodeid 转为 docs contract `50.45s`，后续优化应优先处理
  docs contract scan cost、tail-risk fixture reuse 和 current subscription CLI smoke。
