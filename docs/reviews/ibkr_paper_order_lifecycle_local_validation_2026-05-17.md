# IBKR Paper Order Lifecycle Local Validation

最后更新：2026-05-17

关联任务：`TRADING-010A`

## 结论

本机 IBKR Paper order lifecycle 验证通过，`paper-only=true`，
`production_effect=none`。本次只固化 TRADING-010 的人工本机验证结果，不新增交易
能力，不改变任何策略、评分、回测、日报或 production 解释。

本次没有触发 daily-run、replay、dashboard 或 production conclusion。没有发生 fill。

## 连接记录

|字段|记录|
|---|---|
|本机实测连接方式|TWS Paper Trading|
|同等支持入口|IB Gateway Paper Trading|
|host|`127.0.0.1`|
|port|`7497`|
|client_id|`19010`|
|trading_mode|`paper`|
|account id|`DUP***9620`|
|connection_status|`CONNECTED`|

说明：IB Gateway Paper 的常用端口是 `4002`，本次本机样本使用 TWS Paper `7497`。
提交到 repo 的 review 和 fixture 只保留 masked account，不保留完整 account id、
本机 broker 原始订单编号或 broker 凭证值。

## 订单记录

|字段|记录|
|---|---|
|asset_type|`stock`|
|order_type|`LIMIT`|
|time_in_force|`DAY`|
|symbol|`NVDA`|
|side|`BUY`|
|quantity|`1`|
|limit_price|`10.0`|
|broker_order_id|`[REDACTED_LOCAL_BROKER_ORDER_ID]`|

## 生命周期状态

|检查点|本机验证状态|
|---|---|
|submitted|`true`；订单已提交到 IBKR Paper session|
|openOrder|`open_order_seen=true`；未提交 raw openOrder payload|
|orderStatus|`PendingSubmit -> PendingCancel -> ApiCancelled`|
|cancel|`cancel_requested=true`|
|final cancelled|`final_order_status=ApiCancelled`，`cancelled_confirmed=true`|
|fills|`fills_seen=false`，`filled=0.0`，`avg_fill_price=0.0`|

`lifecycle_status=PASS`。本机报告显示 safety checks 均为 `PASS`，未记录 issues。

## 生产影响边界

- `production_effect=none`
- `paper_order_lifecycle_enabled=true` 只来自本机 local 配置
- 未触发 daily-run
- 未触发 replay
- 未触发 dashboard
- 未生成 production conclusion
- 未接入 live account
- 未写入 production portfolio、production scoring、promotion workflow 或 dashboard 摘要

## 脱敏说明

`tests/fixtures/ibkr_paper_order_lifecycle_sanitized.json` 是提交用脱敏样本。该样本
使用 masked account，移除现金明细，移除原始 broker 订单编号，并只保留 lifecycle
状态、订单字段、安全边界和无 fill 结论。
