# OPS-068：2026-07-21 Daily Gap Recovery

最后更新：2026-07-24

状态：`DONE`（limited non-PIT 历史事实证据完成；strict daily evidence 仍缺失）

稳定任务 ID：`OPS-068_2026_07_21_DAILY_GAP_RECOVERY`

Owner 决策 ID：
`owner_decision:OPS-068:2026-07-24:approve_limited_non_pit_reconstruction_v1`

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
2. `DONE`：项目 owner 于 2026-07-24 选择 limited reconstruction：
   - 提供或定位 5 个 cutoff-compatible exact archive，进入 strict cache-only
     replay validation 的 strict 方案未获选择；
   - 已批准隔离的 `LIMITED_NON_PIT_RECONSTRUCTION`，只保留可验证的
     market/macro 事实，PIT/SEC/OpenAI 保持 null，不生成任何投资结论；
   - 永久不恢复任何 strict 输入的处置未获选择。
3. `DONE`：建立 `limited_non_pit_reconstruction.v2` reviewed schema、
   可重复 producer、content-derived validator 和 tamper tests。它只接受既有
   cache-only inventory bundle，重新校验冻结 bytes、source checksum、日期覆盖、
   null contract 与禁止下游消费的边界；不得调用 provider/OpenAI。
4. `DONE`：在新的隔离目录生成 2026-07-21 reconstruction bundle，运行
   market/macro DQ，并验证所有 canonical cache、2026-07-22/23 state/ledger 和
   global latest/discovery artifacts 均未变化。
5. `DONE`：完成 producer/validator focused tests、task-register consistency、
   artifact catalog/system flow 更新、bundle content validation，并同步任务状态。
6. 只有 strict required inputs 全部存在并
   通过 visibility/DQ/PIT/SEC/OpenAI 验证时，才可讨论历史 replay conclusion。

## 执行设计与序列

1. Producer 只读取明确传入的 inventory bundle，不使用 `latest`、glob 或
   canonical daily discovery pointer；输出位于
   `outputs/replays/limited_non_pit_reconstruction/2026-07-21/<bundle_id>/`。
2. Producer 校验 inventory schema、`as_of`、`mode=cache-only`、
   `inventory_only=true`、`status=INCOMPLETE_REPLAY`、严格缺失项和 frozen
   market/macro input bytes；然后将其复制到新的隔离 bundle，并运行相同
   data-quality code path，但报告和 metadata 只写在 bundle 内。
3. `limited_non_pit_reconstruction.v2` 把 FMP forward PIT normalized/fetch/
   validation、SEC fundamentals 和 OpenAI prereview 明确设为 null，并把
   `daily_score`、`position`、`decision_snapshot`、`dashboard`、
   `reader_brief` 与所有投资结论保持 null。
4. Validator 从 bundle 真实 bytes 复算每个文件的 size/SHA、market/macro
   摘要、DQ status、null contract、forbidden output absence 和 owner decision；
   不信任 producer 在 JSON 中自报的 PASS。
5. 任何 input drift、路径越界、symbol/date 缺口、DQ FAIL、null contract
   漂移、forbidden output 或 canonical side effect 都 fail closed，不发布
   canonical/latest pointer。

## 验收标准

- 原有 `2026-07-22`、`2026-07-23` FAILED state/ledger 原字节保留；
- `2026-07-21` 不出现伪造的 canonical daily PASS；
- 所有恢复输入有明确 path、size、SHA-256、as-of/cutoff 与 provenance；
- 缺失或不确定事实保持 null / `INSUFFICIENT_DATA`；
- 不调用 live provider/OpenAI，不写 production/active shadow weights，不触发
  broker/order/trading action；
- `production_effect=none`。

## 2026-07-24 执行结果

Owner 决策后生成：

`outputs/replays/limited_non_pit_reconstruction/2026-07-21/limited_non_pit_reconstruction_2026-07-21_20260724_ops068/`

结果：

- schema=`limited_non_pit_reconstruction.v2`；
- status=`LIMITED_NON_PIT_RECONSTRUCTION`；
- canonical daily evidence=`MISSING`；
- reconstruction conclusion=`INSUFFICIENT_DATA`；
- isolated market/macro DQ=`PASS_WITH_WARNINGS`，errors=0、warnings=1、
  info=13，唯一 warning code=`prices_adjustment_ratio_jump`；
- 26 个 primary market facts、3 个 macro facts 全部由 frozen CSV bytes
  content-derived 重算；
- 9 个 source artifacts 均有 path、size、SHA-256；
- 5 个 strict missing inputs 和 11 个 conclusion outputs 全部保持 null；
- 4 个 canonical market/macro 文件以及 2026-07-22/23 state/ledger 共 8 个
  guard files 在生成前后及最终外部复核时 byte-identical；
- 独立 validator 11 checks PASS；focused parallel tests 4 passed，并覆盖 market
  fact、frozen input 和 owner decision tamper；
- bundle 无 score、position、Decision Snapshot、Dashboard、Reader Brief 或其他
  conclusion member；全局没有 2026-07-21 canonical state 或 score row；
- 未调用 provider/OpenAI，未发布 canonical/latest/report-registry pointer，未写
  production/active shadow weights，未触发 broker/order/trading，
  `production_effect=none`。

本任务的 owner-approved limited 路径已完成并归档。若未来取得五个 exact
cutoff-compatible archives，必须以新任务执行 strict recovery；不得静默升级本 bundle。
