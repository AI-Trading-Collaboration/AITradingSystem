# Research Window Catalog

最后更新：2026-06-19

状态：`WINDOW_CATALOG_READY_WITH_HOLDOUT_BLOCKER`

本目录冻结当前可用于 development / diagnostic / mini-backfill 的窗口用途。它不冻结 final
untouched holdout，因为已有 stress casebook、TRADING-471~485 诊断窗口和 2026-06-17
回测证据已经被反复使用，不能冒充独立 holdout。

|window_id|start_date|end_date|purpose|forbidden_stage|
|---|---|---|---|---|
|ai_cycle_development_full|2022-12-01|2026-06-18|development context only|final untouched holdout|
|normal_market_regime|2023-01-03|2023-07-31|development / mini diagnostic|final untouched holdout|
|rapid_drawdown|2024-07-10|2024-08-09|stress diagnostic|final untouched holdout|
|slow_drawdown|2025-01-02|2025-04-30|stress diagnostic|final untouched holdout|
|high_volatility_sideways_market|2023-08-01|2023-11-15|stress / turnover diagnostic|final untouched holdout|
|ai_semiconductor_correction|2024-03-08|2024-04-19|AI semiconductor diagnostic|final untouched holdout|
|false_risk_off_cluster|2023-09-01|2023-10-31|false risk-off diagnostic|final untouched holdout|
|untouched_temporal_holdout|BLOCKED|BLOCKED|final independent holdout|all development/tuning/diagnostic stages|

所有数据相关运行必须先调用 `aits validate-data` 同一路径，并在输出中披露 data quality status。
