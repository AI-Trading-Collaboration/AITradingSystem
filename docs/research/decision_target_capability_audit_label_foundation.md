# Decision Target Capability Audit：第一批 Label Foundation

- 结论：`LABEL_FOUNDATION_READY`
- 数据质量：`PASS`
- DQ capability：`decision_target_label_core`
- Full canonical DQ：`FAIL`
- Global cache PASS claim：`False`
- 研究窗口：`2021-02-22` 至 `2026-07-24`
- 共同交易日：1362
- 成熟标签行：5412
- Primary target：`QQQ_MINUS_SGOV`
- Diagnostic controls：`SPY_MINUS_SGOV`、`QQQ_MINUS_SPY`

## Horizon coverage

| Horizon | Rows | Decision start | Decision end | Latest label end |
|---|---:|---|---|---|
| 1d | 1361 | 2021-02-22 | 2026-07-23 | 2026-07-24 |
| 5d | 1357 | 2021-02-22 | 2026-07-17 | 2026-07-24 |
| 10d | 1352 | 2021-02-22 | 2026-07-10 | 2026-07-24 |
| 20d | 1342 | 2021-02-22 | 2026-06-25 | 2026-07-24 |

## 解释边界

- 本批次只定义标签，不训练模型、不选择特征、不搜索策略、不改变仓位。
- 收益区间从 decision close 到第 h 个未来共同交易日 close；`label_available_on_session` 明确标签何时才可用于训练。
- 后续 fold 必须 purge 与验证/测试区间重叠的训练标签，并执行 label maturity gate。
- Embargo 的数值尚未治理，第二批不得自行加入隐含天数。
- 左尾字段是未来路径诊断，不是本批次的策略通过阈值。
