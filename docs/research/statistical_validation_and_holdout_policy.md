# Statistical Validation And Holdout Policy

最后更新：2026-06-19

状态：`VALIDATION_POLICY_FROZEN`

默认市场阶段为 `ai_after_chatgpt`，默认研究起点为 2022-12-01。2022-12-01 之前数据只能用于
warm-up、压力测试或 regime comparison，不能作为 AI-cycle 主结论窗口。

## 必需方法

- Purged walk-forward
- Leave-one-regime-out
- Parameter neighborhood stability
- Block bootstrap
- Medium / high cost assumptions
- Worst-window penalty
- Tracking error budget
- Turnover budget

## Holdout 规则

已反复使用的 stress casebook、historical diagnostics、TRADING-471~485 窗口和已有 backfill
结果必须标记为 development / diagnostic set，不得继续作为最终独立 holdout。

实际 untouched temporal holdout 必须在 candidate full-backfill 前冻结，并在冻结后保持
do-not-touch-until-final。若无法保证未触碰，full-backfill 必须 blocked。
