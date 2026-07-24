# OPS-068：2026-07-21 Daily Gap Recovery

最后更新：2026-07-24

状态：`BLOCKED_OWNER_INPUT`

稳定任务 ID：`OPS-068_2026_07_21_DAILY_GAP_RECOVERY`

## 背景

`2026-07-21` 是 U.S. equity trading day，但本地没有对应的 canonical
`aits ops daily-run` state、run bundle 或正式日报。根因是旧 automation 在
`07:30 Asia/Tokyo` 触发，早于 `19:00 America/New_York` provider-ready
边界；调整到 `09:30 Asia/Tokyo` 后，下一次 resolver 从 `2026-07-20`
直接前进到 `2026-07-22`，没有自动回补 `2026-07-21`。

历史日不能通过当前 live provider refetch 或普通 `aits ops daily-run --as-of`
补造 contemporaneous 证据。恢复必须保持 `2026-07-21` canonical daily
status 缺失，并使用隔离、cache-only、可见性受限的 replay 路径盘点证据。

## 已完成的 inventory preflight

2026-07-24 执行：

```text
aits ops replay-window
  --start 2026-07-21
  --end 2026-07-21
  --mode cache-only
  --inventory-only
  --allow-incomplete
  --continue-on-failure
  --openai-replay-policy cache-only
  --label daily-gap-inventory-20260724
```

隔离 bundle：

`outputs/replays/2026-07-21/replay_window_2026-07-21_2026-07-21_20260724T104706Z_daily_gap_inventory_20260724_20260721/`

结果为 `INCOMPLETE_REPLAY`。该命令没有运行 score、health、secret scan 或
任何下游结论生成器，没有调用 live provider/OpenAI，没有修改 canonical
state/ledger、cache、weights、portfolio 或 broker state。

### 可冻结证据

- 主价格、Marketstack 第二行情源和 FRED rates 可按 `as_of` 过滤并冻结；
- replay download manifest 可由隔离后的 market/macro bytes 生成；
- PIT raw manifest 可冻结 3,907 条、截止 `2026-07-21T22:34:32Z` 的记录；
- valuation snapshots、risk-event occurrences、trade theses 和历史
  features/scores seed 可按 replay visibility contract 冻结；
- OpenAI prereview queue 有 5 条 cutoff-compatible rows，但缺对应正式报告，
  因此不能单独作为完整 prereview 证据。

### 缺失的 strict 必需输入

1. `data/processed/pit_snapshots/fmp_forward_pit_2026-07-21.csv`；
2. `outputs/reports/pit_snapshots_validation_2026-07-21.md`；
3. `outputs/reports/fmp_forward_pit_fetch_2026-07-21.md`；
4. `data/processed/sec_fundamentals_2026-07-21.csv`；
5. `outputs/reports/risk_event_prereview_openai_2026-07-21.md`。

缺少任一上述输入都不能运行 strict replay score，也不能生成或传播
daily score、position、Decision Snapshot、Dashboard、Reader Brief、weekly、
governance、promotion、backtest、weight 或 production 结论。

## 后续步骤与依赖

1. `DONE`：完成 cache-only inventory preflight 并冻结缺口清单。
2. `BLOCKED_OWNER_INPUT`：项目 owner 选择处置路径：
   - 提供或定位 5 个 cutoff-compatible exact archive，进入 strict cache-only
     replay validation；
   - 明确批准隔离的 `LIMITED_NON_PIT_RECONSTRUCTION`，只保留可验证的
     market/macro 等事实，PIT/SEC/OpenAI 保持 null，不生成任何投资结论；
   - 接受 `2026-07-21` 永久为 `INSUFFICIENT_DATA`。
3. 若 owner 选择 strict archive recovery，先验证 source path、size、SHA-256、
   available/captured time、as-of identity 和 lineage，再运行 cache-only replay；
   任一 drift 或 cutoff violation 必须停止。
4. 若 owner 选择 limited reconstruction，必须先建立独立 output scope、schema、
   null contract、source checksum 和禁止下游消费的验证；不得复用 canonical
   daily producer 或 latest pointer。
5. 完成所选路径后同步更新 task register；只有 strict required inputs 全部存在并
   通过 visibility/DQ/PIT/SEC/OpenAI 验证时，才可讨论历史 replay conclusion。

## 验收标准

- 原有 `2026-07-22`、`2026-07-23` FAILED state/ledger 原字节保留；
- `2026-07-21` 不出现伪造的 canonical daily PASS；
- 所有恢复输入有明确 path、size、SHA-256、as-of/cutoff 与 provenance；
- 缺失或不确定事实保持 null / `INSUFFICIENT_DATA`；
- 不调用 live provider/OpenAI，不写 production/active shadow weights，不触发
  broker/order/trading action；
- `production_effect=none`。
