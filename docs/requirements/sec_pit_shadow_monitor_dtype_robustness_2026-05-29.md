# SEC PIT Shadow Monitor Dtype Robustness

任务 ID：`TRADING-048`

最后更新：2026-05-29

## 背景

`RISK-016` 和 `TRADING-047` 修复后，完整 `aits ops daily-run` 已能通过
`score_daily` 和 `sec-pit shadow-observe --latest`。最新 observe summary 已确认默认
baseline 改为 research-only
`data/processed/research/scores_daily_backfill_sec_pit_2023_2026.csv`，baseline coverage
约 99.88%，`shadow_status=OK`。

新的阻断发生在 `sec-pit shadow-monitor --latest` 的 rolling metrics 计算：

```text
NotImplementedError: eq not implemented for <class 'pandas.StringDtype'>
```

触发点是 `_top_rank_label_delta` 中使用 `values.loc[baseline_rank.idxmin()]` 和
`values.loc[observe_rank.idxmin()]` 从混合 dtype DataFrame 取整行。真实 shadow scores
中既有数值 rank/label，也有 string / Arrow string 字段；在部分 Pandas dtype 状态下，
`.loc` 的行选择会触发内部 dtype equality 比较并抛出裸异常，导致 monitor 没有机会生成
结构化 fail-closed 报告。

## 目标

1. `_top_rank_label_delta` 只使用数值化 rank 和 label 序列计算 top-rank label delta，
   不再依赖混合 dtype DataFrame row lookup。
2. Arrow string / pandas string dtype 输入下 monitor rolling metrics 不应裸 traceback。
3. 保留真实监控门禁语义：coverage gate、rolling metrics availability、factor
   underperformance 和 rollback recommendation 不放宽。
4. 修复后重跑完整 `aits ops daily-run`；若继续失败，报告新的真实门禁原因。

## 非目标

- 不修改 monitor policy 阈值、minimum days、sample count 或 rollback 条件。
- 不把 rolling metrics 缺失改成 PASS。
- 不重写 shadow scores、baseline score 或 SEC PIT upstream artifacts。
- 不修改 production scoring、production weights、active shadow weights 或交易行为。

## 验收标准

- 单元测试覆盖 `string[pyarrow]` / pandas string dtype 输入下
  `_top_rank_label_delta` 可返回数值结果而不是抛异常。
- `tests/trading_engine/test_sec_pit_shadow_monitor.py` 通过。
- 相关 ruff、Black 和 `git diff --check` 通过。
- 完整 `aits ops daily-run` 重新执行并通过，或报告新的真实门禁。

## 进展记录

- 2026-05-29：新增并进入 `IN_PROGRESS`。原因：daily-run 在
  `sec_pit_shadow_monitor` 阶段因 Pandas/Arrow string dtype 与 `.loc[idxmin()]`
  交互抛出裸 traceback，阻断后续 score change、market panel、report index、
  documentation contract、research governance、Reader Brief、ops health 和 secret scan。
- 2026-05-29：第一版修复将 `.loc[idxmin()]` 改为临时数值 DataFrame 后，目标单测和
  direct dispatcher 通过，但完整 daily-run 仍在 Windows 子进程中于
  `DataFrame.dropna(subset=["label"])` 触发 access violation；因此继续收窄为纯
  `numpy` 数组选择，完全避开混合 dtype DataFrame row/indexer 路径。
- 2026-05-29：实现完成并进入 `VALIDATING`。验证通过
  `tests/trading_engine/test_sec_pit_shadow_monitor.py`、
  `tests/trading_engine/test_sec_pit_shadow_observe.py`、相关 ruff/Black/diff check，
  以及真实 `aits ops daily-run` as-of 2026-05-28；`sec_pit_shadow_monitor`
  在最终 run 中 PASS。
