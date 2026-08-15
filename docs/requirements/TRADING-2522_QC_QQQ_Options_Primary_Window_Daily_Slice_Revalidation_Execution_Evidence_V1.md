# TRADING-2522 — QQQ Options 主窗口 daily Slice 再验证执行与证据 V1

- status: `BASELINE_DONE`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- registration base: `5485794ec1aa8b89b8e1d7d8d683d0dcc43b27bb`
- predecessor contracts: `TRADING-2520` / `TRADING-2521`
- production effect: `none`
- broker action: `none`

## 目标

在 Project Owner 已签署的 v4 单次授权下，严格执行一次 QuantConnect QQQ Options 主窗口
daily Slice 零订单 Cloud revalidation，收集 export-safe Results artifact，并通过 2521 admission、
2512 strict Results parser 与 2482 DQ/PIT canonical path 形成可审计结论。

本任务只验证数据交付、属性 transport、1202-session coverage 和 derived aggregate 完整性；不执行
策略选择、真实订单、投资解释、paper/live/broker/production，也不授权日级 engine 自动解锁。

## Owner 授权事实

- token：`owner_decision:TRADING-2520:2026-08-15:authorize_single_zero_order_primary_window_daily_slice_revalidation_v4`；
- owner-decision file SHA-256：
  `f37e778a8f8c71e126efe622ef7d3f659af944164f7c97d82269125fa663e197`；
- owner-decision content SHA-256：
  `d62b681d2fafdea939f30278ae2dca39ab28048973868faa0301c650ea00fcd0`；
- 2521 owner candidate content SHA-256：
  `ef5eb9ea3ea8c1b73d6ca6dda6be31fede13b1cfd0f051541508f7d0789c0e9f`；
- collector authorization content SHA-256：
  `87c3360797d8ae913e4e37a3683460eec14ed960c993714e1ede8cbdf713e33b`；
- admitted-unused receipt content SHA-256：
  `818c6edf7811234c621775c31a13525ba26ecdafbfdd3b15edc7ac77c07f0a49`；
- expiry：`2026-08-21T00:00:00Z`；
- current state：`OWNER_V4_AUTHORIZATION_ADMITTED_UNUSED`、`authorization_consumed=false`、
  `external_action_performed=false`。

上述 admission 仅为纯内存 strict validation，不构成 Cloud run 或证据收集。第一次实际 Cloud run
attempt 无论 completed/failed 都消费授权，之后禁止第二次运行。

## 冻结执行边界

- target project id：`34808569`；
- requested/evaluated range：`2021-02-22..2025-12-02`；
- exchange calendar：`XNYS`；expected sessions：`1202`；
- project code LF SHA-256：
  `88a60874737c1e210f5a2f5ac990d14d0f4de3024a1db8f41edaddf3db6226aa`；
- maximum project mutations / Cloud backtests / orders / fills：`1 / 1 / 0 / 0`；
- result carrier：manual browser download of export-safe Results JSON；
- allowed action order：login → one existing-project mutation → one Cloud backtest → one manual
  Results collection；
- prohibited：API、CLI、HTTP、Object Store、raw option rows/log/export、second project/backtest、
  purchase/subscription、range expansion、investment interpretation、paper/live/broker/production。

## 执行步骤

1. 从 exact registration main 运行 START/LANE preflight，并复核 token 仍 admitted-unused。
2. 使用已登录浏览器打开 project `34808569`，读取当前 project code identity；不读取 cookie、local
   storage、secrets 或其他项目。
3. 仅当 target project、code mutation cap 和项目身份一致时，用 2520 canonical `main.py` 覆盖一次。
4. 启动一次、且仅一次 Cloud backtest；该点击/提交即 first run attempt，立即生成 consumed receipt。
5. 等待 terminal，记录 build/engine/backtest id、requested/evaluated range、orders/fills 和错误事实。
6. 仅通过页面的 Results 下载收集 export-safe JSON；禁止 raw option rows、logs download 和其他载体。
7. 运行 2521 strict parser 与 2482 DQ/PIT evaluator；FAIL/UNKNOWN/NOT_EVALUATED 均保持 cash preservation。
8. 更新本 requirement/task projection、system flow/Atlas disclosure 和 canonical evidence；final gates 后
   ordinary push/cleanup。

## 验收标准

1. action ledger ordinal、timestamp、project/code/backtest/result identity 完整且 canonical。
2. project mutation ≤1、Cloud backtest =1、orders=0、fills=0；第二次 run fail closed。
3. token 在 first run attempt 后标记 consumed，即使 Cloud run 失败或 Results 不完整。
4. Results artifact file/content checksum、backtest id、1202 session identity和所有 required derived series
   由 2512 parser 验证，不接受调用者自报 PASS。
5. DQ/PIT 仅由 2482 canonical 15-check path 派生；UNKNOWN 永不产生 PASS。
6. 成功也只可支持 `GO_FOR_DAILY_ENGINEERING_ONLY` 候选评审，不自动激活 selection/engine。
7. 任意越权、订单/成交、raw transport、range/code/scope mismatch 输出 typed failure 并停止。
8. tracked evidence、task registry/shadow、system flow、Atlas 和正式验证保持可重放。

## Path claims

Task-owned：

- `docs/requirements/TRADING-2522_QC_QQQ_Options_Primary_Window_Daily_Slice_Revalidation_Execution_Evidence_V1.md`；
- `inputs/research/qqq_options/trading_2522_primary_window_daily_slice_revalidation_execution_v1/**`；
- `tests/test_qqq_options_daily_slice_revalidation_execution_evidence.py`；
- `src/ai_trading_system/qqq_options_research/daily_slice_revalidation_execution_evidence.py`。

Coordinator-owned：task registry/index/shadow、`docs/system_flow.md`、architecture/DevEx/compatibility
authority 与 Atlas page-effectiveness consumer。

## 临时工作区生命周期

- owner：`TRADING-2522`；
- absolute path：`D:\\Work\\AITradingSystem_trading2522_evidence`；
- purpose：隔离封存 v4 单次 Cloud run 的 export-safe Results、typed failure admission 与
  lane-focused validation，避免触碰并行 TRADING-2523 dirty 主 checkout；
- creation base / branch：`f876ec853c1431e760bc4cf5b89123265a32080f` /
  `codex/trading-2522-daily-slice-run-evidence`；
- exit condition：2522 final candidate 已通过正式五级并 ordinary push，canonical evidence 哈希已验证，
  checkout 无活跃 runner 且无唯一未保存内容；随后审计并移除该 worktree、执行 `git worktree prune`；
- recoverability：移除前由 task commit、pushed main 与 canonical evidence package 提供恢复；任一条件未满足
  时保留并在本 requirement 记录 blocker。

## v4 执行事实与证据结论

- QuantConnect project / backtest：`34808569` / `60ce7e0bec3ad2d83a4d1341e0221492`
  （`Logical Red Bison`）；
- build / engine / host（signed-in Results UI 观察）：`2095dc-5e494a` /
  `2.5.0.0.18004` / `Community B-MICRO`；
- run：`2026-08-15T02:07:06Z..2026-08-15T03:51:41Z`，state=`Completed`，
  orders/fills=`0/0`，Start/End Equity=`100000/100000`；
- export-safe Results：`813386` bytes，file SHA-256=
  `45e8647f4d4b0e3590252acedacca4235695341574f44bc593d8ab9b283f603e`；只下载
  Results JSON，未下载/记录 raw option rows 或 Logs，未用 API/CLI/HTTP/Object Store；
- terminal：`INVALID_INCOMPLETE`，observed/invalid=`0/1202`；daily Slice 确实出现于
  `1201` sessions，但 valid candidate=`0`、transport rejected=`1201`、aggregate chart
  series=`0`；
- 2521 canonical strict parser 从真实 Results bytes 得出
  `DAILY_SLICE_RESULT_PARSER_REJECTED`，而不是接受调用者声明；typed failure=
  `DAILY_SLICE_TRANSPORT_ALL_SESSIONS_REJECTED_UNRESOLVED_AXIS`；
- 当前证据只证明所有实际 chain sessions 都在组合 transport gate 被拒绝，不能从既有诊断可靠区分
  quote、Greeks、OI、volume 等子轴，因此不得伪造具体 root cause；
- external action lifecycle=`COMPLETE/PASS` 只表示四个已授权动作按序且未越界，不会提升
  evidence admission；Results admission=`FAIL`，local aggregate 与 option-event DQ/PIT 均为
  `NOT_EVALUATED`，engine 继续 `POLICY_BLOCKED_CASH_PRESERVATION`；
- v4 authorization 已在 first run attempt 消费，`further_cloud_run_authorized=false`，本任务禁止
  第二次 Cloud run。

## Canonical evidence identities

- owner decision file SHA-256：
  `f37e778a8f8c71e126efe622ef7d3f659af944164f7c97d82269125fa663e197`；
- failure receipt content SHA-256：
  `43711b1ec803d098cb9c6af8f85373ce312ac079d37481e279c1c7659829e4ac`；
- run-attempt consumption content SHA-256：
  `5c997360241d6cc13c21bcf2dbde897b3965554953bb3dee69377c2588c9bca2`；
- complete external-action ledger content SHA-256：
  `406f788b6e9dabfbd8bf552c9a4b20c9a77e39b7fe07b7a9cd87d2c2c3671fce`；
- package manifest content / file SHA-256：
  `d35c932b3508ce5f9c21a86660e5a7bb264574608e5fd756fbc8b9386c8a9137` /
  `407a9f9e8fe6a71dab62c879566b40213116606798e2ea9ba846dd4d05683892`。

Package loader 重建 Owner admission、run-attempt ledger/consumption、complete action ledger、strict
parser rejection 与 failure receipt，要求 non-symlink exact inventory、canonical JSON bytes、artifact
byte count/SHA 和 cross-record content hashes 全部一致；任意 fake PASS、tamper、extra file、result
identity/range/order/equity/runtime drift 均 fail closed。

## Focused failure-fix validation

- 首轮相同并行覆盖：`8 passed / 8 failed`；失败根因仅为 evidence 目录内 Python
  `__pycache__` 破坏 exact inventory，以及 strict `Literal[date]` 无法从 canonical ISO date JSON
  replay；无 parser、授权、DQ/PIT 或现金保护语义失败；
- 最小修复：builder 移入 task-owned source module，evidence package 继续严格 exact inventory；date
  改为 typed field + exact PRIMARY range validator，不放宽日期边界；
- 第二轮完全相同命令：
  `python -m pytest -n 16 --dist loadfile tests/test_qqq_options_daily_slice_revalidation_execution_evidence.py`
  → `16 passed in 25.44s`；Ruff 与无落盘 bytecode compile 均 PASS。

## 当前状态

`OWNER_V4_AUTHORIZATION_CONSUMED_RESULT_INVALID_INCOMPLETE`。task-owned failure evidence 已形成并通过
lane-focused replay，canonical task projection、system flow、Atlas disclosure 与 generated authority 由
同一 latest-main coordinator candidate 收口。后继 `TRADING-2528` 只登记为 per-axis transport 离线诊断
合同，不会在本任务实现，也不授权新的 Cloud run。任何新 Cloud run 都必须先完成该合同并重新取得 exact
Owner token；2522 本身永久保持 second-run blocked、evidence=`FAIL`、DQ/PIT=`NOT_EVALUATED` 与 cash
preservation。

## Coordinator integration worktree 生命周期

- owner：`TRADING-2522` 单一 coordinator；
- absolute path：`D:\\Work\\AITradingSystem_trading2522_integration`；
- branch：`codex/trading-2522-evidence-integration`；
- purpose：从 final latest local main 形成单一 coordinator candidate，吸收已验证 2522 lane，登记
  registration-only `TRADING-2528_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_DIAGNOSTIC_CONTRACT_V1`，
  重建 task registry/system flow/Atlas/generated/compat authority并运行一次正式五级；不实现 2528；
- exit condition：candidate 已 ff-only 合入 local main、ordinary push/双 SHA verify 完成，canonical evidence
  已保留且 runner=0；随后审计并移除 integration worktree、task/integration branches并 prune；
- recoverability：移除前由 2522 lane commit、final task commit、pushed main 与 canonical evidence 提供；
  任一条件不满足时保留并记录 blocker，不删除唯一内容。

## Shared consumer failure-fix validation

- 首轮 2522 evidence + Atlas policy/renderer 同覆盖为 `20 passed / 17 failed in 91.64s`；17 个失败
  全部由 `StrategyResearchPageEffectivenessManifest` 仍冻结 40-task exact count，而 reviewed coverage
  已加入 2522 与 registration-only 2528 后为 42 引发的级联；2522 evidence node 无失败。
- 最小 contract fix 只把 public manifest 与 renderer 的 exact/unique task coverage 从 `40` 提升到
  `42`，保留 schema、task order、freshness、acceptance track、human-review 与 safety 状态机不变。
- 第二轮同覆盖 `36 passed / 1 failed in 125.34s`，剩余为 renderer exact HTML test 仍断言
  `data-task-coverage-count=40`；同步为 42 后第三轮 `36 passed / 1 failed in 118.66s`，仅旧
  “错误的 OptionContract underlying accessor” 文字断言不再匹配当前 v4 next-step。
- 第四轮 `36 passed / 1 failed in 128.14s` 证明 2520 历史 disclosure 的真实原文为
  “误用了 `contract.underlying`”并显示 `underlying_last_price`，而不是此前假设的短语；exact test 改为
  校验这两个实际历史事实，没有删除 2520 coverage。
- 第五轮完全相同命令：
  `python -m pytest -n 16 --dist loadfile tests/test_qqq_options_daily_slice_revalidation_execution_evidence.py tests/atlas/test_page_effectiveness.py tests/atlas/test_cited_query_renderer.py`
  → `37 passed in 119.63s`。所有前轮均作为 focused failure-fix lineage，不作为正式 tier evidence。

## Final pre-formal authority evidence

- 2520–2522 数据链 + Atlas policy/renderer/historical 邻接覆盖：`126 passed / 1 skipped in
  110.02s`；唯一 skip 是隔离 integration worktree 尚未 hydrate ignored canonical page，本地 policy、
  manifest、renderer 与 historical consumer 语义均执行并通过。
- Ruff、strict mypy（4 source files）与 compileall PASS。
- `docs/system_flow.md` 增加 2522 真实 run/failure flow 后，DEVX-006D 首次 build 按 source seal
  fail closed；以最终真实 bytes 更新的 system-flow seal 为 `2215513 bytes /`
  `4547de742edddd9e25cb584728e18b7a3e9b1c45b91fb6457860ad743674ecdc` / git blob
  `5ba33c33c9b5a44b076a09da7929c82ba026fc1f` / `996 entries`。重建结果为 `2919 entries /
  192 fragments`，三 target byte-identical、coverage=100%、silent drop=0。
- compatibility/deprecation 首轮完整同覆盖为 `210 passed / 1 failed in 485.08s`；唯一失败是新增
  1 source module 与 1 test file 后 frozen inventory id/count 仍为旧值。显式以本 worktree
  `PYTHONPATH=src;.` 重算为 `arch_004g_deprecation_inventory_c1296372b72b495eb021`、
  `1121 modules / 1282 tests / 856 writers`，未改变任何 removal gate 或 legacy surface 计数。
- 完全相同命令
  `python -m pytest -n 16 --dist loadfile tests/test_arch_004_refactor_policy.py tests/test_arch_004g_deprecation.py`
  第二轮为 `211 passed in 459.62s`；首轮仅作为 focused failure-fix lineage，不是正式 tier evidence。

## 正式 Architecture failure-fix lineage

- final candidate 首轮 canonical Architecture 为 `864 passed / 1 failed in 481.55s`；artifact：
  `outputs/validation_runtime/architecture-fitness_20260815T054318Z/test_runtime_summary.json`；
- 唯一失败 node：
  `tests/test_arch_005_s5_task_source_cutover.py::test_repository_canonical_registry_is_active_and_self_hosted`；
- 根因是 registration-only `TRADING-2528` 已使 canonical registry exact task count 从 `993` 增至
  `994`，但自托管 exact-count 测试仍冻结旧值；2522 evidence、Atlas consumer、compatibility 与
  DQ/PIT/cash-preservation 语义均无失败；
- 最小修复只同步该 exact-count authority，并重建由测试字节变化影响的 generated manifests/current
  hashes；不放宽 registry 校验、不实现 2528、不授权新 Cloud run。首轮 Architecture 仅作为
  failure-fix parent，不作为 promotion evidence；修复后同一失败文件完整并行覆盖为
  `7 passed in 74.14s`，final tree 必须从 Architecture 起重跑完整五级。

## Reviewed serial ID reconciliation

- 2523 frozen dirty checkout 已实际写入另一条独立 Atlas successor authority；为避免任何双重 task id，
  reviewed coordinator 决定保留 Atlas 规划编号，并把本任务尚未发布的 transport diagnostic 改为
  完全未占用的
  `TRADING-2528_QC_QQQ_OPTIONS_DAILY_TRANSPORT_PER_AXIS_DIAGNOSTIC_CONTRACT_V1`；
- 旧 candidate `a6bc86cab9a052e55ac67d3fc145f05a8775cfe8` 仅保留可恢复审计，不作为 formal 或
  promotion authority；新 candidate 从 frozen base 重放 2522 lane，只通过 canonical task writer
  登记 2528，并完整重建 task registry/system flow/Atlas/generated/compat bindings；
- reconciliation 不读取、覆盖或吸收 2523 dirty bytes，不实现 2528，也不授权 Cloud run。所有正式
  五级必须从 reconciliation final tree 重新开始。
- reconciliation focused 首轮为 `82 passed / 4 failed in 129.26s`：其中 3 个 node 仅因
  DEVX-006D exact test authority 仍冻结旧 `2915 / 992 / old system-flow SHA`，而 canonical writer
  已验证 `2919 / 996 / af6c62d58255f85dd2537cb505c25b3b2f2a16ff5ec995796de864a47b42d1c5`；
  第 4 个 node 是尚未提交 reconciliation tree 时 ignored canonical page 仍绑定旧 candidate。
  修复仅同步 DEVX exact authority；页面必须在新 tracked commit 后由 canonical writer 重建，不删除
  sidecar、不改 skip/freshness/fail-closed 条件。DEVX-006D 完整同文件复测为
  `15 passed in 11.73s`；该首轮仅作 focused failure-fix lineage。
