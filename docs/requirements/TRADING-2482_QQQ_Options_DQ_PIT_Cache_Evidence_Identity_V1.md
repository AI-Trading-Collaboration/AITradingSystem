# TRADING-2482：QQQ Options DQ / PIT / Cache / Evidence Identity V1

最后更新：2026-08-02

稳定任务 ID：
`TRADING-2482_QQQ_OPTIONS_DQ_PIT_CACHE_EVIDENCE_IDENTITY_V1`

优先级：`P0`

状态：`BASELINE_DONE`

Owner 决定：
`owner_decision:TRADING-2482:2026-08-02:freeze_fail_closed_dq_pit_cache_evidence_identity_without_unreviewed_thresholds`

退出标识：`QQQ_OPTIONS_DQ_PIT_IDENTITY_V1_FROZEN`

production effect：`none`

broker action：`none`

## 1. 目标

本任务在任何 signal exporter、QuantConnect project adapter、option universe selection、成交模拟或收益回测
之前，冻结 QQQ option-event 的 data quality、point-in-time、cache identity、engine identity 和 evidence identity
判定边界。它提供 deterministic、fail-closed 的离线 evaluator 与 canonical `DQReportRecord`，使后继模块不能
把缺失、未知、过期、同 bar、fill-forward、calendar/mapping drift、cache collision 或 engine/evidence drift
误写为可研究的 PASS。

本任务不访问 QuantConnect，不下载 raw option rows，不执行 cloud backtest，不形成策略收益、合约选择或
成交结论，也不替 Project Owner 决定 quote-age、spread、Greeks/OI freshness 等数值阈值。

## 2. 精确基线与继承 authority

- frozen base：`f1260cd7be35c6810e97f636d1ec9cfaa0c32449`；
- predecessor：`TRADING-2481_QQQ_OPTIONS_SHARED_SCHEMA_POLICY_FREEZE_V1`；
- shared contract SHA-256：
  `c89916ee7c3a4d9979780bf9359b0b39f61a383fe25aaf251e61ae629b43ff6b`；
- shared policy SHA-256：
  `d7cc57a3c38bf0a1efc0553cb8b6e7ff302efc5afd2be3a44e2f04e82a056349`；
- 2482 必须直接消费 `qqq_options_research.contracts` 的 `DQReportRecord`、`DQCheckResult`、
  `DQStatus`、`PITStatus`、`ExportClassification`、`QQQOptionsSafetyBoundary` 与 canonical seal/replay；
- 不修改或重定义 2481 的 12 records、统一 envelope、public enums、content SHA、canonical bytes 或
  safety semantics；breaking change 必须另开 reviewed serial contract wave；
- primary research default 仍为 `2021-02-22`，但本任务不请求或运行任何研究日期窗口。

## 3. 两条 DQ 状态轴必须分离

`aits validate-data` 是 cached market/macro data 的强制 gate；QuantConnect option chain/quote/OI/Greeks
是另一组 provider/event evidence。2482 必须同时保留以下声明，且禁止相互替代：

1. `local_cached_data_gate`：状态、scope、as-of、report locator/checksum；未执行时显式
   `NOT_EVALUATED` 或 `NOT_APPLICABLE_TO_OPTION_EVENT_SCOPE`；
2. `option_event_dq`：由本 evaluator 对 typed observations 逐项产生 `DQCheckResult`；
3. `option_event_pit`：对 signal、selection、intent、submit、eligible fill quote、fill 的时序与 session
   freshness独立判定；
4. report 不得把 local cache PASS 写成 option-event PASS，也不得用 option-event PASS 覆盖 local cache
   FAIL / NOT_EVALUATED。

任何输入或 authority 的 `UNKNOWN`、`NOT_EVALUATED`、missing 或不一致，最终都只能得到
`dq_status=NOT_EVALUATED`、`pit_status=NOT_EVALUATED` 或更严格的 `FAIL`；不能降格为 warning 后 PASS。

## 4. V1 required checks 与 fail-closed reason codes

### 4.1 Chain / quote

- chain present、candidate present、bid/ask 双边齐全；
- missing、single-sided、negative bid、zero ask、crossed quote、future quote、explicit stale quote 均 FAIL；
- `quote_freshness_assessment` 必须是 typed PASS/FAIL/UNKNOWN；UNKNOWN 不能由代码猜测为 PASS；
- V1 不冻结 max quote age、max spread 或 liquidity 数值。测试中的数字只属于 fixture，不是 policy。

### 4.2 OI / Greeks / calendar / mapping

- daily OI 与 prior-day model/Greeks 的 `as_of_session` 必须等于 reviewed exchange calendar 给出的
  selection session 前一有效 session；
- missing、future/same-session、错误 prior session、freshness UNKNOWN 均 fail closed；
- exchange calendar id/version/hash、symbol mapping id/version/hash 必须 present 且与 expected identity 一致；
- weekend/holiday 不得自行平滑为 prior calendar day。

### 4.3 Signal-to-fill PIT chronology

权威严格时序为：

```text
signal_as_of_ts
  < selection_snapshot_ts
  < order_intent_ts
  <= order_submit_ts
  < fill_quote_end_ts
  <= fill_ts
```

- selection 必须位于 signal 后的首个完整可用 minute，具体 session 由 calendar authority 提供；
- same-bar selection/fill、future quote、fill quote 与 submit 同时或更早、fill-forward ambiguity、任何缺失
  event timestamp 均 FAIL 或 NOT_EVALUATED；
- V1 只冻结 chronology gate，不实现 order/fill engine。

### 4.4 Cache / engine / evidence identity

cache identity 至少绑定：

- provider、dataset、underlying/SID、resolution、requested start/end；
- calendar、mapping、normalization policy identity；
- DQ policy、shared contract、repository code、engine identity；
- source artifact checksum availability 与 checksum（若 provider raw checksum 可用）。

同一 cache key 对应不同 canonical identity payload、requested range、mapping/calendar/normalization、policy、
code、engine 或 source checksum 时，必须报告 `CACHE_IDENTITY_COLLISION`。provider raw checksum 不可得时必须
显式 `PROVIDER_RAW_CHECKSUM_UNAVAILABLE`，不能对 raw bytes 伪造本地 checksum；允许绑定合法导出的 derived
evidence checksum，但必须保留其 export classification。

engine/evidence identity 必须核对 expected 与 observed platform、tier、engine、bundle/artifact digest；license
未闭合使用 `UNKNOWN_REQUIRES_LICENSE_REVIEW`，raw option export 仅允许
`QC_ONLY_NOT_EXPORTED / EXPORT_PROHIBITED`。

## 5. Governed policy

`config/research/qqq_options_dq_pit_identity_v1.yaml` 必须 exact-byte 加载并绑定：

- policy id/version/status/owner、owner decision、rationale/intended effect；
- shared contract SHA 与 shared policy SHA；
- required check ids、reason-code taxonomy、exact chronology expression；
- cache identity component list、source-checksum availability states；
- review/expiry condition、validation plan和安全边界；
- 所有尚无 reviewed numeric authority 的 freshness/spread thresholds 显式为
  `UNKNOWN_REQUIRES_POLICY_REVIEW`，loader 拒绝调用者偷填数字或额外字段。

## 6. 实施文件与权属

Task-owned：

- 本 supporting requirement；
- `config/research/qqq_options_dq_pit_identity_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/dq_pit_identity.py`；
- `tests/test_qqq_options_dq_pit_identity.py`；
- `config/architecture/fragments/modules/qqq_options_dq_pit_identity.yaml`；
- `config/architecture/fragments/flows/qqq_options_dq_pit_identity.yaml`；
- 必需的 compatibility/deprecation authority tests。

Coordinator-owned：

- `docs/task_register.md`；
- `docs/system_flow.md`；
- `inputs/architecture/**` generated manifests/inventories；
- `registry/development_tasks_shadow/**` 与 v2；
- formal validation artifacts。

明确不修改：

- 2481 的 `contracts.py`、`policy.py`、`__init__.py` 与 shared policy manifest；
- 任何 QuantConnect project/API/CLI/account、raw data、cloud run、paper/live/broker path；
- contract selection、fill/slippage、accounting、lifecycle 或投资阈值。

## 7. 阶段与验收

### S0：Policy / typed observations / canonical identity

- exact-byte policy loader 与 hash binding 完成；
- typed observation、cache identity、engine/evidence identity 输入拒绝 extra/naive/float/bad hash；
- evaluator 只通过 2481 `DQReportRecord.seal()` 输出 canonical record；
- deterministic replay 与 tamper tests PASS。

### S1：Negative / PIT / property coverage

- missing/single-sided/negative/zero/crossed/stale/future quote；
- OI/Greeks missing、same/future/wrong prior session、freshness UNKNOWN；
- calendar/mapping mismatch；
- same-bar/fill-forward/future-fill quote/chronology missing or reversed；
- cache collision、raw checksum unavailable、engine/evidence mismatch；
- local cache DQ 与 option-event DQ 互不覆盖；
- UNKNOWN 永不产生 PASS；
- focused tests 使用 `pytest -n 16 --dist loadfile` 或 validation-tier 默认并行。

### S2：Architecture / formal closeout

- `docs/system_flow.md` 与 architecture fragments 同步；
- task shadow、DevEx、compatibility/deprecation、generated manifests deterministic fresh；
- Architecture、Contract、Integration、Reproducibility、Full 在最终候选串行运行且 Full 独占；
- task commit ff-only 到 local main，CLOSEOUT PASS 后 ordinary non-force push；
- 最终 `local main = origin/main = candidate`，退出=`QQQ_OPTIONS_DQ_PIT_IDENTITY_V1_FROZEN`。

## 8. Governed execution 与安全边界

- mode：`SINGLE_LANE` dedicated serial contract wave；
- `contract_change=true`；
- branch：`codex/trading-2482-qqq-options-dq-pit-identity`；
- frozen base：`f1260cd7be35c6810e97f636d1ec9cfaa0c32449`；
- 不创建新的 worktree/clone/cache；复用 clean main checkout 建任务分支；
- 后继 `TRADING-2483` 只在本任务 ordinary main push 与 exact-SHA handoff 后启动；
- external Owner token 仍为 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`；
- `research_only=true`，promotion/paper-shadow/production/raw export/strategy execution/cloud pilot 均 false，
  `production_effect=none`、`broker_action=none`。

## 9. 进度记录

- 2026-08-02：2481 已在 exact main `f1260cd7...` 完成普通 push并释放合同边界；2482 supporting
  requirement 登记并进入 governed serial contract wave。当前仅授权离线工程，未执行任何 external
  platform action。
- 2026-08-02：offline baseline 已实现并进入 `BASELINE_DONE`。DQ policy SHA-256=
  `1e0128c40d9b125f5ce7d2a264f8308eb6885af70ebbdfe2184fed9af85b2358`；focused DQ/PIT=32 PASS，
  与 2481 shared contract 相邻组合=60 PASS，完整 compatibility/governance 组合=239 PASS。15 个 required
  checks、canonical cache receipt、local-cache/option-event 双状态轴、UNKNOWN-not-PASS 与 external no-effect
  boundary 均已覆盖；后继只允许在 exact-main handoff 后消费本 baseline。
