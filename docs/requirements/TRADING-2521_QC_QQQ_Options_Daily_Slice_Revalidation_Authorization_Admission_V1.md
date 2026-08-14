# TRADING-2521 — QQQ Options daily Slice 再验证授权准入 V1

- status: `IN_PROGRESS`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- contract change: `true`
- registration base: `2dc9171ad5f56fc0a9c31b5d388c7d37eb499b8b`
- predecessor: `TRADING-2520_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SLICE_ZERO_ORDER_REVALIDATION_V1`
- production effect: `none`
- broker action: `none`
- external action: `none`

## 问题与目标

TRADING-2520 已在 ordinary-pushed main
`2dc9171ad5f56fc0a9c31b5d388c7d37eb499b8b` 冻结 daily Slice 修复包：项目代码改用
LEAN `OptionContract.underlying_last_price`、canonical `Slice.on_data`、precise daily end time、
reviewed `XNYS` 1202-session identity 与 export-safe diagnostics。2520 只生成 unsigned v4 Owner
request，不包含真实 Owner token admission、single-use consumption 或 Results/DQ-PIT lifecycle。

既有 TRADING-2517 admission 按设计把 2516 v2 token、旧 refresh policy/package 和旧 project-code
hash 固定为 exact authority，不能消费 2520 v4。修改 2517 历史 policy、放宽 token identity 或把本地
dry-run 文本当作 Owner 授权都不允许。本任务建立最小 versioned successor adapter：

1. 严格解析 Project Owner 当前对话实际提供的 2520 v4 token bytes；
2. exact-bind 2520 pushed main、policy/package/proposal/scope/project-code、project/range/session/cap/reviewer/expiry；
3. 复用 2514 action ledger、2517 first-run consumption、2512 Results parser 和 2482 canonical DQ/PIT；
4. token、run、Results 或 DQ/PIT 任一未发生或不通过时保持 typed cash preservation；
5. 为后续唯一一次 bounded zero-order Cloud revalidation 提供可审计入口。

本任务的离线实现不构成 Owner 授权，不允许 QuantConnect login、project mutation、Cloud run、Results
collection、API/CLI/HTTP/Object Store、raw options export、paper/live/broker/production 或投资解释。

## 冻结 2520 authority

- ordinary-pushed main：`2dc9171ad5f56fc0a9c31b5d388c7d37eb499b8b`；
- registration boundary：`54e43a1aa9787c52d4b0cb363e30e5a4bf79aed9`；
- revalidation policy file/canonical SHA-256：
  `f9f859568e34c836a2453b175dc283cbdeec7a009887f6f868beccaabd14f35c` /
  `fc665f68e9fc6bbf52fdb0a3bc903aca13800cb2acdc22d5dd8bd0acd81588b3`；
- package manifest file/content SHA-256：
  `c6d632c0813b47d3a4e96a98457a43403387b79c6c90e214bd9fe1ddb66ee605`；
- proposal / run-scope content SHA-256：
  `d17db4d8944483f6066011c5a854600ea2fdac4a23e91e8b869870c6795e85bb` /
  `7d20c370edfb7653da799444d08b9ceb713c33072f33e4eb3e1f2b7535fbfb14`；
- corrected project-code LF SHA-256：
  `88a60874737c1e210f5a2f5ac990d14d0f4de3024a1db8f41edaddf3db6226aa`；
- target project / range / session count：`34808569` /
  `2021-02-22..2025-12-02` / `1202`；
- maximum project mutations / cloud backtests / orders / fills：`1 / 1 / 0 / 0`。

2518 v3 与更早 token 均已消费或失效，不能复用。2520 v4 token 尚未由 Project Owner 提供；
`owner_token_observed=false` 必须保持真实事实。

## 实施步骤

### S0 — registration boundary

- canonical task row + 本 supporting requirement；
- task shadow、DevEx 与 current authority validate；
- ordinary non-force push registration boundary；
- 从该 exact latest main 创建 task branch 并运行 START/LANE preflight。

### S1 — v4 authorization admission

- task-owned policy、strict loader、typed authorization candidate/receipt；
- duplicate-key、extra/missing field、non-canonical bytes、hash/timestamp/expiry/source mismatch fail closed；
- 只有 `PROJECT_OWNER_CURRENT_CODEX_DIALOG` 来源的实际 bytes 才可产生 admitted-unused receipt；
- validation 不消费授权、不表示 login/project/run 已发生。

### S2 — lifecycle adapter

- 复用 2514 action types/ledger 和 2517 first-run consumption，不复制历史 parser；
- 第一次实际 Cloud run attempt 无论 completed/failed 都消耗 token，禁止第二次 run；
- project mutation、run、manual Results collection 全部受 exact ordinal/chronology/cap/identity 约束；
- Results 只经 2512 strict parser，DQ/PIT 只经 2482 canonical 15-check path。

### S3 — fail-closed coverage

- unit/property/golden：canonical replay、key permutation、admitted-unused、consumed-first-run；
- old/local/expired/reused token、main/package/code/project/range/session/cap/reviewer mismatch；
- second mutation/run、chronology rollback、missing Results、order/fill/raw/prohibited action；
- DQ semantic `FAIL/UNKNOWN/NOT_EVALUATED`、scope/as-of/checksum mismatch；
- 所有 negative/default 路径输出 no-order/no-fill/cash-preservation。

### S4 — shared wiring 与 closeout

- system flow、architecture fragments、task registry/shadow、DevEx/compatibility authority；
- Atlas 仅披露“v4 admission contract 已实现但 Owner token/run/evidence/DQ-PIT 尚未发生”；
- focused/adjacent/compatibility 与 final five-tier gates；
- ordinary push、SHA verify、branch/worktree cleanup。

## 验收标准

1. 不修改 2517/2514 frozen history，不重定义 2481/2482/2512/2520 public semantics。
2. 2520 v4 token 只能从 Project Owner 当前对话的真实 bytes 派生 admission；本地模板不构成授权。
3. validation 与 consumption 分离；第一次 run attempt 后禁止 reuse，即使 run/evidence/DQ 失败。
4. exact main/package/code/project/range/1202 sessions/caps/reviewer/chronology 全部 cross-bind。
5. Results parser 与 DQ/PIT evaluator 直接复用 canonical path；调用者自报 PASS 不可用。
6. `UNKNOWN/NOT_EVALUATED/FAIL`、tamper、scope/as-of/checksum mismatch、order/fill/raw/prohibited action
   不能产生 admission PASS。
7. 不新增 DTE/moneyness/delta/spread/OI/volume/freshness/fee/slippage/latency/partial-fill 等阈值。
8. `owner_policy_value_count=0`、`selection_authorized=false`、orders/fills=`0/0`、engine=
   `POLICY_BLOCKED_CASH_PRESERVATION`。
9. 离线实现期间 external action、production effect、broker action 全部为 `none`。
10. focused、generated/compatibility、Atlas 和 final five-tier gates PASS 后 ordinary push/cleanup。

## Path claims

Task-owned：

- `docs/requirements/TRADING-2521_QC_QQQ_Options_Daily_Slice_Revalidation_Authorization_Admission_V1.md`；
- `config/research/qc_qqq_options_daily_slice_revalidation_authorization_admission_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/daily_slice_revalidation_authorization_admission.py`；
- `tests/test_qqq_options_daily_slice_revalidation_authorization_admission.py`。

Coordinator-owned：task registry/index/shadow、`docs/system_flow.md`、architecture fragments、DevEx、
compatibility/current-authority 与 Atlas page-effectiveness consumer。

## 当前 blocker

`OWNER_V4_TOKEN_NOT_PROVIDED`。该 blocker 不妨碍离线 successor contract 实现，但在 Project Owner
提供 exact token 前，真实 authorization/run/Results/evidence/DQ-PIT 均未发生，external action 必须为
`none`。

## 进度记录

- `2026-08-14T17:21:57Z`：READ_ONLY audit 确认 latest main/origin=
  `2dc9171ad5f56fc0a9c31b5d388c7d37eb499b8b`、checkout clean、lease=0、2521 未占用；
  确认 2517 exact-bound 2516 v2，不能安全消费 2520 v4，因此登记最小 versioned successor。
