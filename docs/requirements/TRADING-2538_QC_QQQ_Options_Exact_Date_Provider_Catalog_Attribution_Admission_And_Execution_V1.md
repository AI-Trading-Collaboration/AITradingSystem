# TRADING-2538 — QQQ Options exact-date provider catalog attribution admission and execution V1

- priority: `P0`
- status: `BLOCKED_EXTERNAL`
- governed mode: `SINGLE_LANE`
- predecessor: `TRADING-2537`
- production effect: `none`
- broker action: `none`
- external boundary: one existing-project mutation and one zero-order QuantConnect Cloud backtest

## 1. Owner authorization fact

Project Owner 于 2026-08-21 在当前 Codex 对话中发送完整 single-use token：

- exact UTF-8/LF token byte count：`2519`（末行后无额外换行）；
- token SHA-256：`1eb650a088217448cd6fbdbc0c54f32f862fe419bd136462dd3f1d941cff21c2`；
- source：`PROJECT_OWNER_CURRENT_CODEX_DIALOG`；
- ordinary-pushed main：`02a3a9b75f9e3e25f0c811cbdb974c1eb5727eb3`；
- target existing project：`34808569`；
- requested/evaluated research range：`2021-02-22..2025-12-02`；
- XNYS sessions：`1202`；expected final never-chain sessions：`1`；
- maximum provider query attempts / project mutations / Cloud backtests / orders / fills：`1 / 1 / 1 / 0 / 0`；
- expiry：`2026-08-21T16:45:19Z`；single-use，首次 project mutation 或 Cloud run attempt 即不可逆消费。

Raw Owner token 不提交到 Git。admission receipt 只记录 byte count、SHA-256、source、admitted time、
expiry、exact authority bindings 与未消费状态。

## 2. Authorized lifecycle

仅授权以下严格有序生命周期：

1. 从 exact ordinary-pushed main 校验 token、2537 package replay、全部绑定 hash、expiry 和未消费事实；
2. 封存 `OWNER_AUTHORIZATION_ADMITTED_UNUSED` receipt；
3. 通过 Owner 现有已登录 Chrome 会话打开 existing QuantConnect project `34808569`；
4. 恰好一次把 existing project 的 `main.py` 修改为 2537 sealed candidate 的 exact `26223` LF bytes，
   SHA-256=`86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4`；
5. 恰好一次提交 zero-order Cloud backtest；在解释结果前先封存 run-attempt consumption receipt；
6. 只收集 reviewed terminal statistics、bounded run metadata、orders/fills 与 source Results JSON 的
   byte count/SHA-256；raw Results 保留在 ignored replay workspace，不提交 Git；
7. 严格校验 exact-date attribution contract，封存 export-safe evidence、action ledger 和 manifest。

首次 project mutation 或 Cloud run attempt 无论成功、失败、平台错误或后续证据校验结果如何都消费授权；
不得自动重试第二次 project mutation、provider query 或 Cloud run。

## 3. Continually prohibited

- project creation、第二次 save/mutation、第二次 run、range expansion、purchase/subscription；
- order、fill、paper/live、broker、production 或 portfolio action；
- raw option rows、contract identifier、strike、expiry、right、bid/ask/last、Greeks、IV、OI、volume；
- Logs-as-data、Object Store、network/API/CLI/direct-HTTP alternative 或从日志重建 raw value；
- 将本次结果解释为 DQ/PIT admission、selection/engine activation、strategy validity、return/risk、
  deployability 或 investment conclusion。

若 login 不可用、project identity drift、token/package/hash/expiry 不匹配、浏览器无法提供 reviewed action
surface，或唯一一次 attempt 失败，必须 fail closed；不得换用未授权 carrier 或 retry。

## 4. Admission and result contract

Admission 必须验证 Owner token 中全部 38 个字段的 exact key/order/value，拒绝 placeholder、duplicate、
missing、extra、reordered、noncanonical、expired、future-dated、wrong-source 与 trailing-newline token；并要求
local `main`、`origin/main` 都等于 token-bound ordinary-pushed SHA。首次浏览器 mutation 前必须存在 sealed
unused receipt。

Result 只允许 2537 run scope 声明的 15 个 `TRADING2537_*` terminal statistic 加 bounded identity metadata。
必须同时满足：

- target session count=`1`，observed session count=`1202`，provider query attempt count 不超过 `1`；
- exact source date match required，cross-date fallback 不能分类为 catalog available；
- execution terminal 与 attribution terminal 分离；
- attribution 仅允许 `EXACT_DATE_CATALOG_AVAILABLE_SUBSCRIPTION_MISSING`、
  `EXACT_DATE_CATALOG_EMPTY`、`NO_EXACT_DATE_PROVIDER_EVIDENCE`、`PROVIDER_PROBE_ERROR` 或
  `ATTRIBUTION_INDETERMINATE`；
- orders/fills/fees/holdings/trading volume 全为零，raw/log/Object Store carrier 全部拒绝；
- `selection_authorized=false`、`production_effect=none`、`broker_action=none`、
  `POLICY_BLOCKED_CASH_PRESERVATION` 保持不变。

## 5. Path claims

Task-owned：

- `docs/requirements/TRADING-2538_QC_QQQ_Options_Exact_Date_Provider_Catalog_Attribution_Admission_And_Execution_V1.md`；
- `config/research/qc_qqq_options_exact_date_provider_catalog_attribution_execution_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/exact_date_provider_catalog_attribution_execution.py`；
- `tests/test_qqq_options_exact_date_provider_catalog_attribution_execution.py`；
- `inputs/research/qqq_options/trading_2538_exact_date_provider_catalog_attribution_execution_v1/**`。

Coordinator-owned：canonical task registry/index/generated views、相关 deterministic task-count/current-source
expectations、`docs/system_flow.md`、Atlas successor disclosure、generated architecture/report-flow/compatibility/
deprecation authority、formal validation artifacts、integration、ordinary push 与 cleanup。

Known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 始终不读取、hash、stage 或修改。

## 6. Acceptance criteria

1. Exact Owner token 与 ordinary-pushed 2537 package 只 admission 一次；浏览器 mutation 前 sealed unused receipt 存在。
2. External counters 永不超过 project mutations / Cloud backtests / provider queries / orders / fills=`1/1/1/0/0`。
3. First mutation/run attempt 创建不可逆 consumed receipt；任何失败不触发第二次 attempt。
4. Evidence 只包含 reviewed terminal aggregates、bounded metadata 与 exact checksums；raw/log/Object Store carrier fail closed。
5. 结果明确 target date、window position、exact/non-target counts、cross-date fallback、typed attribution 以及独立 execution/attribution terminals。
6. 不改变 DQ/PIT、selection、engine、cash preservation、production、broker 或 investment boundary。
7. Focused、generated/compatibility 与适用 final formal validation 在最终证据 tree PASS。
8. 普通 push 后验证 `local main = origin/main = candidate`；临时分支安全清理。

## 7. Temporary workspace lifecycle

Raw Results（若浏览器成功下载）只允许进入 task-owned ignored replay workspace：
`outputs/external_validation/trading_2538_exact_date_provider_attribution_once_20260820/`。该目录在当前任务完成前
用于 source hash replay；tracked evidence 只保留 raw payload 的 byte count/SHA-256，不包含 raw rows。
退出条件是 tracked evidence 与 source hash 经最终验证且 Project Owner/后续 DQ-PIT coordinator 对永久保留作出
决定。达到退出条件前保留并报告路径；达到后须先审计 unique content、active process 与 recoverability，再按
exact absolute-path allowlist 清理。不得删除浏览器原始下载以换取更干净的 closeout。

本任务分支：`codex/trading-2538-exact-date-provider-attribution-execution`；不创建额外 Git worktree 或 clone。

## 8. Progress

- 2026-08-20：Owner token 已收到；offline pre-admission 核对 token 2519 bytes / SHA-256、expiry、
  local/origin main、2537 package replay 与全部绑定 file/content hashes，均匹配。external counters 仍为
  `0/0/0/0`，尚未打开 QuantConnect mutation surface。
- 2026-08-20：sealed unused admission receipt 在 `2026-08-20T17:38:41.741069Z` 生成并 replay PASS，
  content SHA-256=`c673ff52c7e888aa25e9bd6b5a3586c6340d1cd8906f684588d9fe60d88b1b2a`。
  Chrome 已登录账号、target URL/project id 和项目名均只读确认匹配。
- 2026-08-20：唯一整文件 mutation attempt 于 `2026-08-20T17:45:09.302Z` 发起；浏览器 clipboard
  已绑定 exact `26223` LF bytes / SHA-256
  `86a3560f973c7720ac1362757d08e7263845bf3c9b0db51d0690740e54ee3fe4`，随后执行一次
  `Control+A` / `Control+V` / `Control+S`。Cloud Terminal 显示 save/build，但 post-save read-only
  Monaco text 与 screenshot 仍显示前序 marker
  `schema=qc_qqq_options_daily_transport_per_axis_runtime.v2`，没有显示 2537 candidate。clipboard
  copy-back 不是可信验证，因为 synthetic copy 未改变预置 clipboard；视觉/DOM authority 与其冲突后按
  fail-closed 采用 visual/DOM 结论。
- 2026-08-20：按 Owner request 的“首次 project mutation 或 run action 无论成功失败均不得自动重试”
  边界，authorization 已消费；没有再次 paste/save，没有提交 Cloud backtest。初始
  `mutation_consumption_receipt.json` 是 save 后、read-only verification 前生成的 preliminary receipt，
  已由 `mutation_attempt_incident.json` 显式标记
  `SUPERSEDED_BY_POST_SAVE_READ_ONLY_VERIFICATION_FAILURE`，不得作为实际 mutation 成功证据。
  terminal counters 为 project mutation attempts / verified mutations / Cloud backtests / provider queries /
  orders / fills=`1 / 0 / 0 / 0 / 0 / 0`；incident SHA-256=
  `ccbfb1b522900514c5472f46e2c6709d9f1d9c48fa276161aaea78b1ee21b4af`，ledger SHA-256=
  `2fca0a59e815754c3c0051d60ed2b41177e0b8fceaec2d77f1664bf2ecd1b2ba`，blocked manifest
  SHA-256=`a1631f1afa5d73c98c2e237d9af9576104e37cc47e02625db4bd507e29bb3b78`。
- 2026-08-20：未产生 Results JSON，因此 task-owned ignored replay workspace 未创建；没有 raw result、
  下载文件或临时 cache 需要保留/清理。下一步只能由 Project Owner 在审阅本 incident 和下一次可靠的
  single-input Monaco mutation mechanism 后发放新的 exact single-use token；当前 token 不授权 retry。
- 2026-08-21：外部终态证据固化后的 focused parallel validation 为 `96 passed`；report-catalog-flow、
  DevEx、compatibility 与 canonical task-source authority validate 均为 `PASS`。最终提交仍须通过适用的
  formal validation、exact-commit Atlas successor replay、integration preflight 与 closeout audit；这些步骤
  只发布失败事实，不会恢复、替换或扩大已消费的外部授权。
- 2026-08-21：Architecture 首轮发现新增 module/test 造成 frozen deprecation inventory 的 repository
  counts 与 inventory id 漂移（`1 failed, 864 passed`）；确认所有 deprecation surface 均无变化后，更新
  inventory counts/id 与 deterministic expectation。重跑 Architecture=`865 passed`，Contract=`276 passed`，
  Integration=`995 passed`，Reproducibility=`24 passed`。对应 runtime artifacts 分别为
  `architecture-fitness_20260820T184231Z`、`contract-validation_20260820T184700Z`、
  `integration_20260820T184940Z`、`reproducibility_20260820T185047Z`；提交绑定后的 Full 仍为最终 gate。
