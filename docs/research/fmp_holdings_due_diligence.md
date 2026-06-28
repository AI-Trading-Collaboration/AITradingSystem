# FMP Holdings Due Diligence

YAML：`inputs/research_reviews/fmp_holdings_due_diligence.yaml`

## 当前判定

`FMP_NOT_SUITABLE`

当前本机可见 `FMP_API_KEY`。脱敏最小 capability check 访问 QQQ holdings endpoint
时返回 `HTTP_402_PAYMENT_REQUIRED`，未保存原始响应，也未记录 API key。

这说明当前 entitlement 不能完成 FMP holdings 能力验证。该结果不能证明 FMP 永远不
提供 holdings，只能说明当前 key / plan 下不可用，且不得自动升级。

## PIT blocker

即使后续 owner 批准升级或 trial，FMP holdings 仍必须证明：

- historical QQQ holdings 可按日期查询；
- `holding_date`、`reported_date`、`known_at` 可区分；
- 退出成分和 delisted securities 可追踪；
- 覆盖 `2021-02-22` 起主窗口；
- license 允许本地缓存和派生 feature。

在这些条件满足前，FMP holdings 只能是 `diagnostic_only`，不能成为 model-ready
breadth。
