# OPS-079 Historical Daily Gap Recovery Executor

最后更新：2026-09-05

稳定任务 ID：`OPS-079_HISTORICAL_DAILY_GAP_RECOVERY_EXECUTOR`

Owner 指令：`owner_instruction:OPS-079:2026-09-05:continue-blocked-historical-gap-recovery`

状态：`IN_PROGRESS`

## 1. 问题与证据

OPS-070 S3 已生成并验证 `daily_input_capture_recovery_queue.v1`，但 reviewed baseline 明确没有自动或人工 recovery executor。2026-09-05 的 cache-only inventory 复核确认：

- 最新 session `2026-09-04` 已是 canonical daily `PASS 34/34`；
- 历史 ledger 仍有 `CAPTURED=20`、`PARTIAL_CAPTURE=10`、`MISSED=1`；
- queue 共 12 项，其中三个 `market_macro` 为 `READY_FOR_MANUAL_RECOVERY`，一个 `sec_companyfacts` 为 `OWNER_REVIEW_REQUIRED`，其余八项为 `INSUFFICIENT_DATA/HISTORICAL_RECAPTURE_FORBIDDEN`；
- 五个目标日 cache-only replay 均为 `INCOMPLETE_REPLAY`，没有任何日期具备完整 strict PIT archive；
- `2026-09-02` SEC 相邻 retained capture 的 17/17 payload hash 相同，但后一份是在 decision cutoff 后取得，只能形成 non-PIT review。

因此，本任务只建立隔离、content-derived、人工触发的历史 source evidence executor；它不修订 canonical daily history，也不把缺失 strict PIT evidence 补造成可消费结论。

## 2. 首批授权范围

|recovery id|session|component|允许状态|
|---|---|---|---|
|`daily-input-recovery-86e4fc43ebe265eb9853`|2026-08-18|`market_macro`|`IMMUTABLE_RAW_BACKFILL_EVIDENCE`|
|`daily-input-recovery-3936c8e25aafa6b7fd45`|2026-09-01|`market_macro`|`IMMUTABLE_RAW_BACKFILL_EVIDENCE`|
|`daily-input-recovery-d857f6e26833865746fe`|2026-09-02|`market_macro`|`IMMUTABLE_RAW_BACKFILL_EVIDENCE`|
|`daily-input-recovery-94f13cd67dc6a3b3f8b3`|2026-09-02|`sec_companyfacts`|`MANUAL_NON_PIT_RAW_REVIEW`|

其余八项 strict source 继续保持 `INSUFFICIENT_DATA`；本任务不允许 provider/OpenAI historical recapture。

## 3. 输入与执行合同

每次执行必须显式提供 recovery queue、recovery id、owner decision id、reviewed policy 和 canonical guard paths。执行器必须先严格验证 queue schema、queue validation、policy version、item identity、action status、recovery mode 与三项全局禁止位：

- `automatic_execution_allowed=false`；
- `historical_strict_pit_backfill_allowed=false`；
- `consumer_cutover_allowed=false`。

`market_macro` 还必须显式绑定一个已存在的 cache-only inventory bundle，并冻结 `replay_run.json`、`input_freeze_manifest.json`、主/副行情、利率与 download manifest 的原始 bytes。`sec_companyfacts` 必须显式绑定 before/after capture manifest 及其引用的 payload bytes，逐 ticker 比较，不得按 `latest`、glob 或 mtime 发现来源。

## 4. 输出合同

每个 bundle 只写入：

`outputs/replays/historical_gap_recovery/<session_date>/<recovery_id>/`

固定成员至少包括：

- `historical_gap_recovery.json` 与 `.md`；
- reviewed policy、queue、queue validation 和 branch-specific source snapshots；
- `canonical_guard_before.json` 与 `canonical_guard_after.json`；
- `historical_gap_recovery_validation.json` 与 `.md`。

目录必须 single-create；既有目标目录、symlink、路径逃逸、unexpected member、duplicate JSON key、non-finite number、source drift 或 guard drift 均 fail closed。Validator 必须从真实 bytes 重算所有 path/size/SHA-256、market/macro exact-date coverage、SEC ticker/hash matrix、derived status、Markdown 与 safety 字段。

## 5. 不变性与安全边界

禁止写入或覆盖：

- `data/raw/daily_input_capture/<date>`；
- `data/processed/daily_input_capture/<date>`；
- canonical daily state/ledger、capture manifest、source state；
- gap ledger、recovery queue 或 queue validation；
- DQ discovery/receipt、score、position、Decision Snapshot、Dashboard、Reader Brief、report registry 或 latest pointer；
- official/active-shadow weights、portfolio、order、broker 或 trading state。

`market_macro` 输出固定 `strict_pit_eligible=false`、`consumer_cutover_allowed=false`；SEC 输出固定 `strict_pit_eligible=false`、`contemporaneous_evidence_status=MISSING`。所有 bundle 固定 `production_effect=none`、`provider_request_performed=false`、`openai_request_performed=false`、`broker_action_allowed=false`、`broker_action_taken=false`、`trading_action_taken=false`。

## 6. 验收标准

1. canonical task fragment、supporting requirement、reviewed policy 与 schema 已登记；
2. producer/validator 对四个首批 item 的 branch contract 可重算；
3. tamper、path escape、existing output、unexpected member、wrong queue item、source drift、canonical guard mutation 和 forbidden safety promotion tests PASS；
4. focused pytest 使用 `-n 16 --dist loadfile`，Architecture、Contract、Integration、Reproducibility 与 Full 全部 PASS；
5. `docs/system_flow.md`、`docs/artifact_catalog.md`、operations runbook、daily runbook、CLI/help 与 compatibility authority 同步；
6. exact release promotion 后才在 permanent runtime 执行首批四项，并逐项运行 content-derived validator；
7. 运行前后 canonical gap ledger、queue、queue validation 与 latest daily state 保持 byte-identical；
8. 历史 canonical `MISSED/PARTIAL_CAPTURE` 与八项 strict `INSUFFICIENT_DATA` 不变。

## 7. Worktree 与 publication 生命周期

- frozen base：`293813e5e2e7b88886b79fc22cf77e2d57f1f346`，当时 local `main=origin/main`；
- branch：`codex/ops-079-historical-gap-recovery-executor`；
- worktree：`D:\Work\AITradingSystem_ops079_historical_gap_recovery`；
- owner：OPS-079 coordinator；
- publication transaction：`ops-079-historical-gap-recovery-20260905-v6`；
- purpose：canonical registration、最小 serial contract wave、实现、正式验证、发布与首批受限恢复；不得吸收 `TRADING-2564` 或其他 lane 变更；
- exit condition：candidate 已进入 local/remote main，exact release 已 promotion，首批四项 runtime bundle 均通过 validator，canonical guards byte-identical，且无唯一未替代的 tracked/untracked/ignored evidence、进程或 lease 依赖；否则记录 blocker、next owner 与恢复边界后保留 worktree。

## 8. 进度记录

- 2026-09-05：stale `TRADING-2559` v5 transaction 已按官方 fence 以 `FAILED` 关闭，lease 已释放；成功 v11 保持 `COMPLETED/RELEASED`，main ancestry 不变。
- 2026-09-05：READ_ONLY audit 显示 active lease 为空，local `main=origin/main=293813e5...`；独立 OPS-079 worktree 已建立，未修改带有 TRADING-2564 未提交变更的 worktree。
- 2026-09-05：取得 transaction `ops-079-historical-gap-recovery-20260905-v1` / lease `lease-f36978e5120787261698`，开始 task-source 与最小 serial contract 登记。
- 2026-09-05：v1/v2/v4/v5 分别因声明闭包、generated authority 与测试预期漂移 fail closed 并正式释放；v3 仅留下未取得 lease 的 immutable intent，未形成 active transaction。
- 2026-09-05：v6 / lease `lease-fd5b9013d358e6d305ba` 已取得；producer/validator、双 CLI、9 项专属测试及 33 项相邻日常采集回归均 PASS，继续执行最终 generated rebuild 与正式门禁。
