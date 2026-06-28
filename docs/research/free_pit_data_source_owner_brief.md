# Free PIT Data Source Owner Brief

## 结论

先补免费数据是为了在不购买供应商数据的前提下，把 rates、VIX 和官方 calendar risk 的 PIT contract 做成可审计输入。

## PIT-approved

- `rates_liquidity_free_v1`：在 DGS2/DGS10/DTWEXBGS 覆盖满足时可用于 research。
- `volatility_compression_free_v1`：VIX index + QQQ realized vol 可用于 research。

## Diagnostic-only

- calendar rows 缺少 `source_published_at` 时只能 diagnostic。
- `participation_proxy_free_v1` 不是 true PIT breadth。

## 仍需付费或 owner 输入的数据

- true PIT breadth / historical constituents / survivorship-free universe。
- analyst revision / earnings estimate PIT data。

## Safety

- readiness status：`FREE_FEATURE_FAMILY_REOPEN_READINESS_READY_WITH_BLOCKERS`
- promotion、paper-shadow、production、broker 全部继续 disabled。
