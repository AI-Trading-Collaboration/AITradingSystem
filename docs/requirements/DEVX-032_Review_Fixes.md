# DEVX-032 发布门禁的三处修补（Codex 评审）

## 1. 背景

2026-10-14 Codex 评审指出三处缺口，已在 main 上逐条核对属实（核对记录见项目文件 `knowledge/claude/ops/codex-review-2026-10-14.md`）。

## 2. owner 决定

owner_decision:GOV-008:2026-10-14:review_fixes_v1：Baker 在项目线程「Codex 评审」中于 2026-10-14 12:51Z 说"还是我们自己来修复这些问题吧"，12:54Z 说"第2个问题 也改下"。三处都改，做成一个包。

## 3. 改动

1. **门禁后复检工作区**：`ship.py` 的 `gate()` 和 `ship()` 在测试跑完后、写 Gate 行之前，用 `tree_clean_exclusions` 再查一次工作区。测试期间有任何未提交修改，就报"working tree changed while the gate ran"，不写 PASS，不移动 main。以前只比较提交树，不看工作区，一步式 `ship` 会把测过别的代码的结果记成通过。
2. **研究数据层进 C 区**：`src/ai_trading_system/research_data/**` 和 `config/research/qqq_long_history.yaml` 划进 zone C。快照 id 只钉原始 CSV；`derive_daily`、`repair_stale_levels`、`check_snapshot` 决定每条收益序列和检查状态，运行记录里只有 CSV 和规格的哈希。以后改它们要 owner 的决定记录。取代 2026-10-14 `bench_config_zone` 里"research_data 仍在 B 区"的那一句。
3. **只改任务卡的提交走任务门禁**：策略加 `task_records: [tasks/*.yaml]` 和 `gates.tasks`。一个提交的 B 区路径全是任务卡（其余是 A 区）时，门禁跑 `tools/tasks.py check`（全部卡校验、渲染视图是最新）和 `tests/test_tasks_register.py`，不跑整个产品测试。任务卡仍在 B 区，只是门禁换了；不改成 A 区，因为 A 区门禁为空。近 120 个提交里有 18 个只因任务卡就跑了完整测试。

## 4. 验收

- 新测试：`ship`、`gate` 在测试期间改了文件都不写 PASS 且 main 不动；只改任务卡加 A 区文件走任务门禁，混入代码走完整门禁；策略里没有这两个键时不变；真实策略下 `research_data/**`、`qqq_long_history.yaml` 为 C、`tasks/*.yaml` 为 B。
- 变异：去掉复检调用、让任务门禁条件恒真、把任务门禁条件改成不可达，对应测试都失败。
- 全套 PR 测试通过。
