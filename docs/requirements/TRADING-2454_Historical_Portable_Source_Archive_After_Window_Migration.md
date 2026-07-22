# TRADING-2454：统一窗口迁移后的历史 Portable Source Archive

最后更新：2026-07-22

状态：`BASELINE_DONE_S0_S1_S2_BLOCKED_EXACT_DATA_BYTES`

稳定任务 ID：`TRADING-2454_HISTORICAL_PORTABLE_SOURCE_ARCHIVE_AFTER_WINDOW_MIGRATION`

## 背景

TRADING-2450 的 canonical portable-lineage sidecar
`portable-lineage_dfa5dfc7208e5913fc75` 冻结了 TRADING-2449 R0/R1/R2 当时使用的 108 个
source bindings。TRADING-2452 按 owner 决策把 active primary window 迁移到 `2021-02-22` 后，
其中两份仍被 active runtime 使用的配置已经合法变化：

- `config/research/strategy_research_restart_policy.yaml`；
- `config/research/controlled_strategy_next_stage_research.yaml`。

四份 immutable R0/R1/R2 artifacts 仍保持原 byte/hash；canonical sidecar 也必须保持冻结。因为当前
project-relative locator 上的 source bytes 不再等于历史 binding，portable validators 现在正确返回
`HISTORICAL_PORTABLE_CONFLICT`。用当前 source 重建 sidecar 会破坏“原结果由原 source 重放”的语义，
不得作为修复。

## 目标与阶段

|阶段|内容|验收|
|---|---|---|
|S0|恢复精确历史 source bytes|从可信历史 commit/archive 恢复上述两份配置在 TRADING-2450 sidecar 冻结时的 exact bytes，并记录来源 commit、size、SHA-256|
|S1|设计 versioned archive locator|历史 archive 与 active config 路径分离；resolver 明确选择 sidecar 绑定的历史 source，不覆盖 active `2021-02-22` 配置|
|S2|重放与 tamper 验证|R0/WF/robustness/R2 四级 validator 全部 PASS；任一 archive missing/hash/size/path drift 继续 fail closed|
|S3|共享收口|artifact catalog、system flow、compatibility/source hashes 与 focused/reproducibility/architecture/contract/full 按风险更新|

## Wave 8 实施冻结

只读 Git 审计已证明初始识别的两份 config 历史 bytes 均唯一可恢复，因此这两份 config 的输入
blocker 已解除；后续全量扫描发现的 data-source blocker 仍未解除：

|binding|历史 source|size|SHA-256|source commit / blob|
|---|---|---:|---|---|
|`source_100e2a05abff91305c13`|`strategy_research_restart_policy.yaml`|6866|`0f81afa123d5646548496951a912b04a00dd4535b77ef60de5760ed6f02bf476`|`20b878ea` / `db706fcd9c6501710a629972ce3450162f31405c`|
|`source_2c9c99f56d401726e639`|`controlled_strategy_next_stage_research.yaml`|49482|`626ad4a44cde8cfd6d29442ad2514aed776689aae77a9f7c878e278969e4156e`|`f7fd3583` / `65dc4e91c519c7ca0831dc4eeea439e6357214b6`|

两 blob 在 sidecar freeze commit=`3156a4b9` 与最后 pre-migration commit=`0fc316e5` 保持 exact，
active-window migration commit=`bc78fb18`。S0/S1 使用独立 versioned archive policy/manifest 和
content-addressed project-relative archive path；manifest 必须绑定 canonical sidecar id/SHA、binding id、
原 locator、历史 SHA/size、source/freeze/migration commit 与 Git blob。Resolver 只在调用者显式提供
`historical_source_archive_manifest_path` 时 overlay 这两个 binding，其余 106 个 binding 仍使用原
locator；未 opt-in 时继续返回 `HISTORICAL_PORTABLE_CONFLICT`。

相对 legacy path 的 controlled-strategy binding 只有在 disposition 精确为
`active_locator_superseded_by_window_migration`，且 sidecar binding/original locator/SHA/size 全部 exact 时
才可切到 archive。Strategy lane 独占 archive policy/manifest/bytes、archive resolver、portable-lineage
resolver/adapters 与 focused tests；task register、requirement、artifact catalog、system flow、generated
manifests/views、compatibility/source hashes 和 formal gates 由 integration coordinator 单写。

S2 必须证明 manifest content id、policy/sidecar binding、path containment、missing/tamper/duplicate/
disposition drift 均 fail closed；默认 conflict 保持；显式 archive opt-in 后 R0/WF/robustness/R2 四级
validator 全部 PASS；active configs、canonical sidecar、四份 legacy artifacts、run IDs 与 R2 decision
前后 byte/semantic identical。任一条件失败即停止，不重建 sidecar、不回滚 active config、不降低门禁，
`production_effect=none`、`broker_action=none`。

## S2 实际结果与新增 blocker

实现后的首轮四级 replay 没有被改写成 PASS：R0 在配置 overlay 后继续以
`PORTABLE_SOURCE_TAMPERED/source_f3af401bd04447415bc1` 停止，WF/robustness/R2 继续以
`PORTABLE_SOURCE_TAMPERED/source_2359a08b2c37809e744c` 停止。全量扫描证明冻结的 108 个 source 中
不是原审计假设的 2 个、而是 7 个与当前 locator 不一致：

|source|冻结 size / SHA|当前 size / SHA|处置|
|---|---|---|---|
|restart policy|6866 / `0f81afa1...f476`|6923 / `473cb6b3...5280`|exact Git blob 已归档并由显式 overlay 解析|
|controlled strategy policy|49482 / `626ad4a4...156e`|49591 / `7a8e490f...e6a`|exact Git blob 已归档并由显式 supersession overlay 解析|
|download manifest|730277 / `3703a1c5...725c`|740975 / `87c067b5...96c8`|当前 append-only prefix 可精确复算冻结 SHA，尚未归档|
|prices daily|5102327 / `b9c96337...5425`|5104015 / `e6ce8943...43b1`|`BLOCKED_EXACT_BYTES_NOT_FOUND`|
|marketstack prices|3230927 / `460b9416...901`|3232464 / `321ace91...864e`|当前 rows 截至 2026-07-17 可精确复算冻结 SHA，尚未归档|
|rates daily|159197 / `d530a35e...2658`|159435 / `e66abc0e...94ee`|`BLOCKED_EXACT_BYTES_NOT_FOUND`|
|forward ledger|7069 / `f5c2c653...d88`|7502 / `202ccfa1...420`|当前 append-only prefix 可精确复算冻结 SHA，尚未归档|

Git objects、`D:\Work\AITradingSystem-eb0-candidate`、repo replay outputs 与 pytest temp snapshots 均未找到
prices/rates 的 exact size+SHA bytes；不得通过下载近似数据、手工删行、当前 provider 重建或放宽 hash 来
伪造历史 replay。现有 baseline 保留 versioned archive contract、两个 exact config blobs、显式 opt-in
resolver/adapters 与 fail-closed tests；未 opt-in 仍返回原 conflict，opt-in 只把 blocker 前移到下一份真实
drift source。专项 focused=`32 passed`，工程 Wave 8 focused=`112 passed/1 skipped`。退出条件是 owner/data
archive 提供 prices/rates exact bytes 或可审计备份位置，随后统一归档其余 5 个 source 并重跑四级 validator；
在此之前本任务按项目规则标记 baseline done + data-source blocker，而不是 DONE。

## 安全边界

- 不改写 canonical TRADING-2450 sidecar、四份 legacy artifacts 或原 decision；
- 不把当前 2021 active config 复制并标记为历史 source；
- 不回滚 active window，不运行 backtest、candidate generation、parameter search 或 prospective holdout；
- 未显式提供 archive 时保持 `HISTORICAL_PORTABLE_CONFLICT`；显式提供当前两源 archive 时在下一份
  未归档 drift source 返回 `PORTABLE_SOURCE_TAMPERED`，不得将任一失败人工替代为 PASS；
- 全部工作 `production_effect=none`、`broker_action=none`。

## 依赖与下一责任方

- 当前依赖可信 data archive 提供 `prices_daily.csv` 与 `rates_daily.csv` 的 exact bytes，并在同一
  manifest 中归档可精确复算的 download manifest、marketstack prices 与 forward ledger；两份 config
  exact bytes 已完成归档；
- strategy evidence platform 负责 source provenance 与 replay，integration coordinator 负责共享文档和
  manifests；
- TRADING-2452 和 TRADING-2453 不依赖历史 replay 恢复，不能用本任务阻塞当前 active-window closeout。
