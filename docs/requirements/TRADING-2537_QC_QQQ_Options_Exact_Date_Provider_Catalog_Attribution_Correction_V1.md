# TRADING-2537 — QQQ Options exact-date provider catalog attribution correction V1

- priority: `P0`
- status: `BASELINE_DONE`（non-terminal；等待 final-tree Full、ordinary publication 与新 Owner token）
- owner: Codex capability coordinator（offline correction）；Project Owner（任何 external action）
- governed mode: `SINGLE_LANE`
- contract change: `true`（provider attribution 的 consumer-visible typed contract）
- registration base: `fb246ab362e6942e3f4948c1e1cd9247212f9897`
- production effect: `none`
- broker action: `none`

## 1. 问题与目标

TRADING-2532 已证明 `2021-02-22..2025-12-02` 的 1202 个 XNYS session 中存在
`1201 PRESENT / 1 MISSING`，但封存 aggregate 没有导出唯一 never-chain 日期。TRADING-2535
因此建立了 zero-order、export-safe attribution proposal，其候选代码在每个 chainless Slice 中
调用 `QCAlgorithm.option_chain(canonical_option_symbol)`，并按返回 contract count 直接归因为
provider catalog `AVAILABLE` 或 `EMPTY`。

该 helper 不是 exact-date availability authority：LEAN 的 chain history 路径在当前 universe 文件
不可用时可以返回最近交易日的可用 universe。2535 没有检查返回数据的 source/end date，因此可能把
前一交易日的 catalog 误归因为目标 session 可用，也会在约 1020 个曾出现 chainless Slice 的 session
上重复执行不必要的 provider history probe。现有静态测试只确认 helper token 存在，没有证明
cross-date fallback 会 fail closed。

本任务作为不可回退 TRADING-2535 sealed proposal 的 successor serial correction，生成独立的
exact-date V2 sealed package。它只修复诊断器和 typed attribution contract，不改写 2532～2536
历史证据，不访问 QuantConnect，不修改 Cloud project，不运行 backtest，不读取或导出 raw option rows。

## 2. 权威输入与不变量

- predecessor local main：`fb246ab362e6942e3f4948c1e1cd9247212f9897`；
- source backtest id：`acf111f24d09a41870f9a23e93fcbe3b`；
- requested/evaluated research range：`2021-02-22..2025-12-02`；
- exchange calendar / exact session count：`XNYS / 1202`；
- expected final never-chain count：`1`；
- 2535 policy、run scope、proposal、project code 与 package manifest bytes 均保持 immutable；
- 2536 Atlas classification 作为历史 proposal/unexecuted 证据保留，不冒充 2537 已执行；
- DQ/PIT、selection、engine、orders、fills、portfolio、production 与 broker 边界不变。

任何 predecessor hash、window、session identity、package inventory 或 safety flag drift 必须 fail closed。

## 3. Corrected exact-date probe contract

候选代码必须分两阶段执行：

1. `on_data` 只记录每个 expected session 的 Slice count、canonical QQQ equity Slice presence 与
   subscribed `Slice.option_chains` non-empty event count；不得在 chainless Slice 中调用 provider history；
2. `on_end_of_algorithm` 先证明 observed session count=`1202` 且 unique final never-chain count=`1`，
   然后仅对该目标日期执行一次 bounded `History[OptionUniverse]` exact-date query；
3. 只把 `OptionUniverse.end_time.date()` 等于目标日期的 record 纳入 exact-date count；任何只返回更早
   日期的结果必须记录为 cross-date fallback 并 fail closed，不得归类为目标日 `AVAILABLE`；
4. 目标日期需要同时导出其窗口位置 `START_BOUNDARY / INTERIOR / END_BOUNDARY`，为后续 pre-roll、
   subscription repair、provider remediation 或 post-roll 路径提供有界证据；
5. provider probe 必须是 count-only：不导出 symbol、contract identifier、strike、expiry、right、
   bid、ask、last、quote、Greeks、IV、OI、volume、raw rows、异常正文、日志载荷或 Object Store 内容。

provider query 的 evaluated interval 必须覆盖目标 session 且不改变 research conclusion window；实际
requested/evaluated date range 必须在 bounded terminal statistic 中显式记录。

## 4. Typed attribution contract

只允许以下归因：

- `EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING`：存在 source date 精确匹配的
  `OptionUniverse` record，exact-date contract count > 0，且 subscribed chain event count = 0；
- `EXACT_DATE_CATALOG_EMPTY`：存在 source date 精确匹配的 record，且 exact-date contract count = 0；
- `NO_EXACT_DATE_PROVIDER_EVIDENCE`：没有精确匹配 record，或只发现跨日 fallback；
- `PROVIDER_PROBE_ERROR`：目标日 bounded query 抛出异常，仅导出稳定 reason code；
- `ATTRIBUTION_INDETERMINATE`：session/window/identity/count 不完整，或不满足以上 exact 条件。

`execution_terminal=COMPLETE` 只表示 session/account/safety 边界完整；必须另行导出
`attribution_terminal=RESOLVED / INDETERMINATE / ERROR`，不得把运行完整误写为归因已解决。以上状态
仅用于 provider availability / subscription transport 归因，不改变当前 `chain_presence=FAIL`、
DQ=`FAIL`、PIT=`NOT_EVALUATED` 与 `POLICY_BLOCKED_CASH_PRESERVATION`。

## 5. 实施步骤与归属

### S0 — Registration / contract freeze

- 建立 canonical task row 与本 requirement；
- 以 exact local-main base 通过 governed `SINGLE_LANE` start/lane preflight；
- 本文即最小 serial consumer-contract correction，2535 sealed bytes 不改写。

### S1 — Offline V2 sealed package

Task-owned：

- `config/research/qc_qqq_options_exact_date_provider_catalog_attribution_correction_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/exact_date_provider_catalog_attribution_correction.py`；
- `tests/test_qqq_options_exact_date_provider_catalog_attribution_correction.py`；
- `inputs/research/qqq_options/trading_2537_exact_date_provider_catalog_attribution_correction_v1/**`；
- 本 supporting requirement 与 task-specific architecture fragments。

Coordinator-owned：canonical task registry/index、generated task views、`docs/system_flow.md`、
Atlas page-effectiveness classification/reader summary、generated architecture/compatibility/deprecation
authority 与 formal validation artifacts。

实现必须生成 deterministic policy、run scope、proposal、candidate `main.py`、unsigned owner request
与 package manifest，并绑定 2535 package identities 和 2532 evidence identities。当前 external counters
保持 `project_mutations/cloud_backtests/orders/fills = 0/0/0/0`。

### S2 — Validation / publication

- unit tests 至少覆盖 exact-date contracts available、exact-date empty record、no record、prior-date-only
  fallback、provider error、target count 非 1、session count 不完整、package replay/tamper rejection；
- candidate compile，静态拒绝 `self.option_chain(...)`、orders、logs、Object Store、network 与 raw fields；
- focused pytest 使用 `-n 16 --dist loadfile`；运行适用 Architecture、Contract、Integration、
  Reproducibility 与 Full final-tree validation；
- 更新 `docs/system_flow.md` 与 Atlas successor classification，明确 proposal ready / external execution
  unexecuted；
- 完成 task terminal event、task branch commit、local-main ff-only、ordinary non-force push、SHA verify
  与 branch cleanup。

## 6. 外部动作与后续真实修复边界

在 corrected package ordinary-pushed 且 Project Owner 对 final exact hashes 发放新的 single-use token 前：

- `maximum_project_mutations=0`；
- `maximum_cloud_backtests=0`；
- `maximum_orders=0`；
- `maximum_fills=0`；
- `external_action=none`。

未来一次获批 zero-order run 只负责定位目标日期和归因。若目标是 start boundary，后继可提出一个
pre-roll trading session 但 evaluation 仍从 `2021-02-22` 开始；若是 end boundary，可提出一个 post-roll
session 但 evaluation 仍止于 `2025-12-02`；若 interior exact-date catalog available，修复 subscription /
transport；若没有 exact-date provider evidence，必须走 provider remediation/backfill 或治理后的替代
primary source，不得 forward-fill、静默排除该日或把 derived seal 冒充 provider evidence。

## 7. 生命周期

- 任务分支：`codex/trading-2537-exact-date-provider-attribution-correction`；
- 本任务不创建额外 worktree、clone、cache 或 supervised-run workspace；
- 当前 checkout 的 known-unrelated exclusion
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、stage 或修改；
- 生成的 sealed package 是 canonical governed evidence，任务关闭后保留；task branch 在普通推送和
  unique-content audit 后删除，可从 local/remote main 恢复。

## 8. 进度记录

- 2026-08-20：READ_ONLY 排查确认 2535 `option_chain` probe 缺少 source-date 校验并可能跨日回退；
  Project Owner 指示继续推进修复。本任务登记为 P0 serial correction，external action 仍为 none。
- 2026-08-20：corrected candidate 已实现：`on_data` 不再查询 provider；完整 1202-session scope 与
  unique target 成立后，只执行一次 `History[OptionUniverse]` target-date query；每个返回 collection
  必须以 `end_time.date()` 精确匹配目标日期，prior-date-only 与 mixed-date response 均 fail closed 为
  `NO_EXACT_DATE_PROVIDER_EVIDENCE`。execution 与 attribution terminal 已分离。
- 2026-08-20：V2 sealed identities：policy file/canonical SHA-256=
  `405e09dbdc58d7037e35de4d047bf4b80f9ced7030e69df96e56c727fb1af8c9` /
  `5abae42535973e59f5064288e091e9c9ddcfdab416bb5eb7e9a40fc321c03229`；run scope
  content/canonical SHA-256=`dc83b410fcb844e6c05193f81b6c46e10359c9cb5af0a2eb83fcf6a26d9a2019` /
  `132f1f8e82d73db8b77e8dd69daced2b12a39ad6cb9d45d365ef46fdbcc60f0a`；proposal
  content/canonical SHA-256=`7ecfda585fb1c84b4967193b624310292bd2efac55ce22e54fa19c79101f95a7` /
  `82a70010a7231ffeb15de833a2926c5032b39fba389af172341ba4d7a79609dd`；project code=
  `26223 bytes` / `86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4`；
  manifest content/canonical SHA-256=`d2cfac9c2b66a9e3e8203537cb2ed2a9bcec5ef6a7d17c9e8d40eee41c4c8737` /
  `9beffa28d1cfcd97548fc14b025dbb05e3e88f4f0ca35f66c6cfd37dbea65118`。
- 2026-08-20：candidate/fallback/package/2535 immutability/Atlas/authority focused 最终为
  `87 passed`（另有 local canonical ignored artifact exact-commit test 按设计留到 candidate commit
  后重建）；Ruff PASS；Architecture 首轮 `862 passed / 3 failed` 与第二轮
  `864 passed / 1 failed` 都是新增 task/module 后的 explicit snapshot/frozen inventory refresh，失败
  artifacts 保留在 `architecture-fitness_20260819T183422Z` 与
  `architecture-fitness_20260819T184611Z`。生成 authority 修正后 Architecture=
  `865 passed`，Contract=`276 passed`，Integration=`995 passed`，Reproducibility=`24 passed`。
- 2026-08-20：final committed tree 的 Full authority 固定为
  `outputs/validation_runtime/trading_2537_full_final_v1/test_runtime_summary.json`；该 artifact 缺失或
  status 非 PASS 时不得进入 local-main integration。当前 external counters 仍为 `0/0/0/0`，没有
  QuantConnect、Cloud、browser、raw rows、Object Store、orders、fills 或 broker action。
- 2026-08-20：提交 `64cd439c4d5aa2c7d73c129689baf0a0259e3002` 的首次 Full 为
  `9243 passed / 4 failed / 3 skipped / 643 warnings`，parent artifact 即上述
  `trading_2537_full_final_v1/test_runtime_summary.json`。四项失败均为生成权威新鲜度缺口：一次
  ARCH-004E aggregate shadow 未在 architecture fragment 尾部格式修正后重建，另三项来自
  DEVX-006C compatibility authority 尚未吸收本任务对 `docs/system_flow.md`、deprecation test 与
  canonical task-source fragment 的当前哈希。exact-date probe 行为、2535 immutable predecessor、
  Atlas exact-commit 页面和其余 9243 项均未失败。修复必须重建并验证这两条 authority，创建新的
  final commit，按新 commit 重建 ignored Atlas sidecars，再以该失败 artifact 为 parent 运行完整
  `failure_fix_rerun`；不得用 focused PASS 代替 Full。
- 2026-08-20：authority-refresh commit `ac432c5cf458f5f5d70c080c71124a8286abfc91`
  的首次 parent-bound Full rerun 为 `9246 passed / 1 failed / 3 skipped / 644 warnings`，artifact=
  `outputs/validation_runtime/trading_2537_full_failure_fix_rerun_v1/test_runtime_summary.json`。此前四项
  新鲜度失败全部通过；唯一新暴露项是 `tests/test_devx_006d_report_catalog_flow_authority.py` 的
  compatibility-consumer 冻结断言仍保留旧 `entry_count=2956`，与同文件及已验证 authority 的当前
  `2960` 不一致。修复仅允许把该精确 consumer 断言同步为 `2960`，刷新由该测试哈希影响的 DevEx /
  compatibility authority，创建新 final commit 并重建 ignored Atlas sidecars；随后必须以本次失败
  artifact 为 parent 再运行完整 Full。
