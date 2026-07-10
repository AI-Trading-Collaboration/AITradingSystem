# ARCH-004A1 CLI 契约基线失败修复

最后更新：2026-07-11

## 任务信息

- task id：`ARCH-004A1_CLI_CONTRACT_BASELINE_FAILURE_REMEDIATION`
- parent：`ARCH-004`
- priority：`P0`
- status：`DONE`
- owner：architecture coordinator
- production effect：`none`

## 背景与根因

ARCH-004A 在 commit `4ca63626` 上运行 full parallel validation：

- command：`python scripts/run_validation_tier.py full --write-runtime-artifact`
- runtime artifact：`outputs/validation_runtime/full_20260710T155603Z/test_runtime_summary.json`
- result：`46 failed / 5305 passed / 643 warnings`
- elapsed：`996.77s`

46 个失败不是 46 个独立实现缺陷。它们共同追溯到 2026-07-09 的 CLI 边界拆分提交 `98c976f0`：原 `research_execution_semantics.py` 中两个已被大量命令依赖的适配器被抽到 `research_execution_common.py` 时发生了契约漂移。

### Root cause A：date-range adapter 漂移

抽取前：

```python
_date_range_kwargs(as_of, start_date, end_date) -> {
    "as_of_date": ...,
    "start_date": ...,
    "end_date": ...,
}
```

抽取后错误地变成：

```python
date_range_kwargs(start, end) -> {"start": ..., "end": ...}
```

现有调用方仍传三个参数，因此 CLI 在进入 builder 前抛出 `TypeError`。即使只把参数个数补回，如果不恢复精确键名，仍会把错误推迟成 builder unexpected-keyword failure。

### Root cause B：as-of adapter 漂移

抽取前输出键为 `as_of_date`，抽取后误改为 `as_of`。下游 runner 的正式参数仍是 `as_of_date`，因此所有只需要 observation date 的 CLI 都抛出 `unexpected keyword argument 'as_of'`。

## 修复边界

本任务只恢复抽取前的公开适配器语义：

1. `date_range_kwargs` 恢复三个显式参数 `as_of/start_date/end_date`；
2. 返回键恢复为 `as_of_date/start_date/end_date`；
3. 未提供 `start_date` 时继续使用项目级 `DEFAULT_AI_REGIME_BACKTEST_START`；
4. `as_of_kwargs` 返回 `as_of_date`；
5. 添加直接 contract regression test，防止以后“只看 helper 单体代码”再次改变调用方契约；
6. 运行全部 46 个原失败 node，以及 full parallel validation。

## 明确不做

- 不逐个修改 46 个 CLI 来适配错误 helper；
- 不向 runner 增加含混的 `**kwargs`；
- 不改变 strategy、threshold、weight、research window 或 market-regime policy；
- 不改变 artifact path/schema/status；
- 不增加 waiver，不用 serial pytest 覆盖 parallel failure；
- 不进入 ARCH-004B Semantic Kernel。

## 实施步骤

1. 固化 helper 的直接 characterization tests；
2. 恢复两个 shared adapter 的原契约；
3. 运行 helper tests 和代表两类错误的 CLI tests；
4. 运行 full baseline 中全部 46 个失败 node；
5. 重跑 full parallel validation；
6. 将 before/after evidence 写回 ARCH-004A reconciliation、baseline 和 task register。

## 验收标准

- helper exact keys、date parsing 和 default start 行为有测试覆盖；
- 46 个原失败 node 在 `-n 16 --dist loadfile` 下全部 PASS；
- full parallel validation 不再出现这两类 `TypeError`；
- 若 full suite 暴露新失败，必须记录新 root cause 和 linked task，不能把 ARCH-004B 解锁；
- `production_effect=none`、`paper_shadow_allowed=false`、`production_allowed=false`、`broker_action=none`；
- task register、ARCH-004 RFC、Phase A baseline 与机器 reconciliation 同步更新。

## 状态记录

- 2026-07-11：从 full parallel baseline 识别并登记为 `IN_PROGRESS`。已用 pre-refactor source、current helper、runner signatures 和 pytest trace 交叉确认两个 root cause；尚未修改运行时代码。
- 2026-07-11：恢复 `date_range_kwargs(as_of,start_date,end_date)` 的 exact keys `as_of_date/start_date/end_date`，恢复 `as_of_kwargs -> as_of_date`，并新增 5 个直接 helper contract cases。两个代表性 CLI 加 helper tests 为 7 passed；原 46 个失败 node 在 `-n 16 --dist loadfile` 下 `46 passed / 0 failed`（101.19 秒）；full parallel validation 为 `5358 passed / 0 failed / 643 warnings`（876.65 秒），artifact=`outputs/validation_runtime/full_20260710T162418Z/test_runtime_summary.json`。未改变 strategy、threshold、weight、window policy、artifact schema 或 production boundary，任务归档 `DONE`。
