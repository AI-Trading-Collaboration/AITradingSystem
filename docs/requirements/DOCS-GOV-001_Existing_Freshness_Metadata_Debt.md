# DOCS-GOV-001：既有文档新鲜度 metadata 债务

最后更新：2026-07-23

状态：`DONE`

## 背景

ARCH-005 S4B 文档收口时运行全局 `aits docs validate-freshness`，扫描 677 个文档并发现 10 个
既有 committed requirements 文档缺失或落后 `最后更新`。这些文件不属于 S4B owned scope；本任务
单独承接，避免在并行协议提交中吸收无关历史内容。

2026-07-23，Wave11 push CI 在同一 `Docs freshness` 前置步骤再次失败；父提交的 push/scheduled CI 已
在相同步骤失败，因此这不是 Wave11 runtime 回归。当前扫描扩大到 688 个文档，并新增识别
`TRADING-2450` 的 metadata 晚于原登记时间，最终修复范围为 11 个文档。

## 发现清单

|文档|问题|
|---|---|
|`docs/requirements/ARCH-004F1_Operations_Control_Plane.md`|缺少 `最后更新`|
|`docs/requirements/ARCH-004F3_Reporting_Architecture.md`|缺少 `最后更新`|
|`docs/requirements/TRADING-102_to_110_Stable_Real_Parameter_Iteration_Backtest_Loop.md`|metadata 早于 2026-07-12 状态记录|
|`docs/requirements/TRADING-111_to_113_Real_Research_Evidence_Closure.md`|metadata 早于 2026-07-11 状态记录|
|`docs/requirements/TRADING-141_to_145_Historical_Advisory_Replay_and_Backfilled_Outcome_Evaluation.md`|metadata 早于 2026-07-12 状态记录|
|`docs/requirements/TRADING-146_to_150_Historical_Replay_Result_Diagnosis_and_Advisory_Rule_Calibration.md`|metadata 早于 2026-07-12 状态记录|
|`docs/requirements/TRADING-156_to_160_Outcome_Update_Loop_and_Rolling_Advisory_Evidence_Refresh.md`|metadata 早于 2026-07-12 状态记录|
|`docs/requirements/TRADING-161_to_168_Backtest_Simulation_Advisory_Evaluation.md`|metadata 早于 2026-07-13 状态记录|
|`docs/requirements/TRADING-169_to_173_Simulation_Result_Interpretation_and_Advisory_Rule_Review.md`|metadata 早于 2026-07-13 状态记录|
|`docs/requirements/TRADING-204_to_208_Real_Manual_Snapshot_Dry_Run_and_Owner_Decision_Loop.md`|metadata 早于 2026-07-12 状态记录|
|`docs/requirements/TRADING-2450_Legacy_Research_Artifact_Portable_Lineage.md`|metadata 早于 2026-07-21 状态记录|

## 实施边界

- 逐文档确认内部最大语义状态日期，不能仅按当前 wall clock 批量覆盖；
- 只修复 metadata 和为解释 metadata 所必需的最小文字，不改变历史状态、策略结论或验收证据；
- 若检查发现内容状态本身冲突，拆出独立任务，不在 freshness housekeeping 中重写结论；
- 不运行数据、回测、周期 operations 或 provider 请求；
- `production_effect=none`、`broker_action=none`。

## 验收标准

1. 上述 11 个文档 targeted freshness 全部 PASS；
2. 全局 `python -m ai_trading_system.cli docs validate-freshness` PASS；
3. `tests/test_docs_freshness.py`、`tests/test_documentation_contract.py` 和 task-register consistency PASS；
4. 任务登记与 shadow registry 同步，`git diff --check` PASS；
5. 不混入其他文档内容重写或系统行为变更。

## 状态记录

- 2026-07-23：11 个目标文档均仅更新一行 `最后更新` metadata；逐组 targeted freshness 分别
  `5/0` 与 `6/0`，全局 freshness=`688 docs / 0 issues / PASS`。未改写历史状态、策略结论、
  runtime、数据或生产语义；任务登记、shadow registry、GOV normalization、compatibility authority
  与 docs/architecture/contract 验证在同一 closeout 中同步，`production_effect=none`，任务转 `DONE`。
- 2026-07-23：父提交与 Wave11 push CI 均在既有 global freshness debt 上失败，任务转
  `IN_PROGRESS`。修复范围由原10个扩为当前扫描确认的11个；只允许 metadata 修正、task/shadow/
  compatibility closeout，不改变任何文档历史结论、runtime、策略、数据或生产语义。
- 2026-07-20：从 S4B 全局文档新鲜度扫描登记为 `P2/READY`。本轮 S4B 自身四个变更文档 targeted
  freshness 均 PASS；10 个问题全部来自未修改的既有文档，留待独立 metadata lane 处理。
