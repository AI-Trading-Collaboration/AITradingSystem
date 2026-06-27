# Data Quality Watchlist

最后更新：2026-06-27

本文记录尚未阻断当前研究结论、但需要后续确认的数据质量 warning。这里的条目不得被静默平滑或从报告解释中移除；如果相关资产进入正式 strategy universe，必须重新评估 blocker 等级。

|ID|资产|日期|warning|当前状态|当前影响|升级条件|跟踪文件|
|---|---|---|---|---|---|---|---|
|TQQQ_ADJUSTMENT_RATIO_JUMP_2025_11_20|TQQQ|2025-11-20|`prices_adjustment_ratio_jump`|`NON_BLOCKING_WARNING`|当前未发现影响 execution semantics actual-path ranking 的证据；不阻断 TRADING-1256～1285 owner review / policy sensitivity|如果 TQQQ 进入正式 strategy universe、paper-shadow candidate universe 或 production-facing report 解释范围，升级为 `BLOCKING_DATA_QUALITY_REVIEW`|`inputs/data_quality_watchlist/tqqq_adjustment_ratio_jump_2025_11_20.yaml`|

## 后续调查要求

TQQQ 2025-11-20 warning 需要确认是否来自：

- 真实拆分、分红或复权事件；
- 数据源调整异常；
- 单日异常价格；
- 复权系数跳变。

调查完成前，任何使用 TQQQ 的正式候选资产池、paper-shadow preflight 或 production-facing 结论都必须显式披露该 warning 的状态。
