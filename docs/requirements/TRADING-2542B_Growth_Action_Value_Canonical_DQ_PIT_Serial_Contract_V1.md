# TRADING-2542B：Growth Action Value Canonical DQ/PIT Serial Contract V1

最后更新：2026-08-23

稳定任务 ID：`TRADING-2542B_GROWTH_ACTION_VALUE_CANONICAL_DQ_PIT_SERIAL_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

Owner 指令：
`owner_decision:TRADING-2542B:2026-08-23:continue_complete_canonical_dq_pit_contract_draft_v1`

该指令授权继续完善 serial contract draft；它不是四个 numeric threshold 的 exact approval，也不是
independent review、DQ run、cache mutation、provider query、backtest、投资结论或交易授权。

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. 背景与目标

TRADING-2542A 已依据 Project Owner 采纳的 GPT Pro 结论建立 measurement-complete V2 exact value
sheet。七个非 DQ axis 已有明确公式；`CANONICAL_DQ_PIT` 仍只能要求 reviewed DATA_RESEARCH gate
exact `PASS`。其中 quote age=`120s`、relative spread=`0.20`、open interest=`10`、volume=`1` 只是
`OWNER_INTENT_ONLY_NOT_EXECUTABLE_AUTHORITY`。

现有 `qqq_options_dq_pit_identity_v1` 与 `qqq_options_staged_dq_pit_readiness_v1` 正确保持四项 numeric
threshold 为 `UNKNOWN_REQUIRES_POLICY_REVIEW`。不得用 2542A draft 静默覆盖历史 authority，也不得在新
DQ 或 strategy result 可见后反推规则。

本任务建立一个独立 versioned serial contract draft，完整定义：

1. quote-age clock、timestamp direction 与 timezone；
2. relative-spread denominator、crossed/single-sided/zero denominator；
3. contributing contract、session 与 primary-window aggregation；
4. missing、UNKNOWN、FAIL、INSUFFICIENT、INVALID 与 PASS 的映射；
5. exact source date、prior-session OI freshness、PIT availability 与 identity；
6. global DQ terminal order。

该 contract 只提供 schema、identity、replay、纯函数和 synthetic/offline tests。它不得读取真实 market
cache、raw option rows、provider、Cloud、backtest 或已有 hypothesis result。

## 2. Scope 与权威边界

### 2.1 输入 lineage

- hypothesis：`BASELINE_BOUNDED_QQQ_GROWTH_OVERLAY_NON_BETA_ACTION_VALUE_V1`；
- selected data lane：`QQQ_OPTIONS_PRIMARY_WINDOW_DERIVED_AGGREGATE_EVIDENCE`；
- research window：`2021-02-22..2025-12-02`，expected sessions=`1202`；
- stage：仅 `DATA_RESEARCH`；不得代签 `SHADOW_SELECTION` 或 `EXECUTION`；
- base identity：`qqq_options_dq_pit_identity_v1` 与
  `qqq_options_staged_dq_pit_readiness_v1`；
- consumer draft：`strategy_growth_action_value_threshold_exact_value_sheet_v2`。

### 2.2 Numeric authority

四个值必须逐项保留 source、unit、direction、rationale、review state 与 execution state：

| field | Owner intent | draft measurement | authority state |
| --- | ---: | --- | --- |
| `max_quote_age_seconds` | `120` | `decision_as_of_utc - quote_end_utc` | `PENDING_OWNER_APPROVAL` |
| `max_relative_spread` | `0.20` | `(ask-bid)/((ask+bid)/2)` | `PENDING_OWNER_APPROVAL` |
| `min_open_interest` | `10` | exact prior exchange-session OI | `PENDING_OWNER_APPROVAL` |
| `min_volume` | `1` | non-negative current source-session volume | `PENDING_OWNER_APPROVAL` |

在 Owner exact approval 与 independent review 均完成前，四项值不得形成 executable DQ authority；
真实 DQ evaluation 必须 fail closed 为 `AUTHORITY_UNAVAILABLE`，而不是暗中采用这些值。

## 3. Exact measurement contract

### 3.1 Quote age 与 relative spread

- 所有 timestamps 必须是 timezone-aware UTC；naive、non-UTC、missing 或 non-finite input 为
  `INVALID`；
- `quote_end_utc` 必须 `<= decision_as_of_utc`；future quote 为 PIT violation=`INVALID`；
- quote age 为 `(decision_as_of_utc - quote_end_utc).total_seconds()`，不得取绝对值、四舍五入或向下
  截断；在 numeric authority 可执行后，`age <= max_quote_age_seconds` 才通过；
- bid 必须 `>=0`，ask 必须 `>0`，ask 必须 `>=bid`；否则 quote integrity=`INVALID`；
- denominator 固定为 midpoint=`(bid+ask)/2`；midpoint `<=0`、non-finite 或 decimal overflow 为
  `INVALID`；relative spread 不得改用 ask、bid、last 或 underlying price 作分母；
- 在 authority 可执行后，`relative_spread <= max_relative_spread` 才通过。

### 3.2 Exact date 与 PIT

- option-universe source date 必须与 target exchange session date 完全相同；cross-date fallback 禁止；
- quote、volume 与 contract identity 必须来自同一 source session；
- open interest 的 freshness rule 固定为 `EXACT_PRIOR_EXCHANGE_SESSION`，current-session、calendar-prior
  day 或 future value 均为 `INVALID`；
- `available_at_utc <= decision_as_of_utc`；缺少 `available_at` 为 `UNKNOWN`，future availability 为
  `INVALID`；
- provider、engine、calendar、mapping、normalization、repository code、source evidence 与 aggregate
  identity 均必须与 reviewed manifest 完全相同，identity drift=`INVALID`。

### 3.3 Contract 与 session aggregation

- `contributing_contract=true` 表示该 contract 的字段实际进入 selected derived aggregate；不得由结果
  反推或在 DQ evaluation 中修改该标记；
- non-contributing rows 只记录 exclusion reason，不得冒充 contributing evidence，也不得单独导致
  session PASS；
- contributing contract 的 identity/PIT/quote integrity violation=`INVALID`；numeric threshold miss=
  `FAIL`；required field missing=`UNKNOWN`；仅全部适用检查通过才为 `PASS`；
- session 至少需要一个 contributing contract；零 contributing contract=`FAIL`；
- 任一 contributing contract `INVALID` -> session `INVALID`；否则任一 `FAIL` -> session `FAIL`；否则
  任一 `UNKNOWN` -> session `INSUFFICIENT`；仅全部 contributing contract `PASS` -> session `PASS`；
- 每个 requested session 必须恰有一个 terminal record；duplicate、out-of-range、unexpected session 或
  expected-session set drift=`INVALID`。

### 3.4 Primary-window 与 global terminal

terminal precedence 固定为：

`INVALID > FAIL > INSUFFICIENT > PASS`。

- 任一 session `INVALID` -> `GLOBAL_INVALID`；
- 否则任一 session `FAIL` -> `GLOBAL_FAIL`；
- 否则任一 session `INSUFFICIENT` -> `GLOBAL_INSUFFICIENT`；
- 仅 exact 1202/1202 requested sessions 全部 `PASS` -> `GLOBAL_PASS`；
- `UNKNOWN` 永远不得映射为 PASS；禁止 majority vote、pass-rate tolerance、silent row/session drop、
  cross-date substitution 或 weighted compensation。

## 4. 实施步骤与路径声明

### S0：registration

- 本 requirement、canonical task event 与 generated compatibility views；
- 更新 parent 2542/2542A，登记 2542B 的 draft/review boundary。

### S1：serial contract draft

- `config/research/strategy_growth_action_value_canonical_dq_pit_contract_v1.yaml`；
- `src/ai_trading_system/strategy_growth_action_value_dq_pit_contract.py`；
- `tests/test_strategy_growth_action_value_dq_pit_contract.py`；
- coordinator 更新 `docs/system_flow.md`、Atlas disclosure 与 generated architecture/task views。

### S2：validation 与 review handoff

- strict YAML/JSON、canonical seal/replay、cross-file identity 与 synthetic positive/negative tests；
- focused pytest 默认 `-n 16 --dist loadfile`，随后运行适用 formal/final-tree gates；
- terminal 保持 `DRAFT_COMPLETE_PENDING_OWNER_AND_INDEPENDENT_REVIEW`；
- independent reviewer 必须核验 contract semantics 与 numeric intent source；Project Owner 必须逐项 exact
  approval。任一修改必须新建版本，不改写 V1 bytes。

## 5. 验收标准

- 六类 required serial fields 0 omission，全部由 typed schema 与 pure evaluator 覆盖；
- numeric intent 与 executable authority 分离，未审批时真实 evaluation 返回 authority unavailable；
- quote clock、spread denominator、exact-date/PIT、OI prior-session、contributing rows、session/global
  precedence 均有 deterministic positive/negative tests；
- duplicate key、unknown field、identity drift、wrong window、wrong session count、hidden action request 和
  terminal tamper fail closed；
- V2 exact sheet bytes 不变，V1/V2/comparator identity 不被重写；
- `dq_run_authorized=false`、`cache_mutation_allowed=false`、`provider_query_allowed=false`、
  `empirical_research_authorized=false`、`backtest_allowed=false`、`production_effect=none`、
  `broker_action=none`；
- task source、system flow、Atlas disclosure 与实现一致，适用 validation PASS。

## 6. 生命周期

- exact base：`675b8841890b9c943d9e57ab9e99509426e00fa2`；
- task branch：`codex/trading-2542b-dq-pit-contract-v1`；
- workspace：复用 `D:\Work\AITradingSystem` 当前受审计 checkout；不创建 worktree、clone、external
  cache 或 validation snapshot；
- purpose：只完成 DQ/PIT serial contract draft、pure evaluator、tests 与治理投影；
- exit condition：draft、focused/formal validation、task/source/system-flow 同步，普通推送到 main，删除已合并
  task branch；
- retained state：ignored Atlas canonical page 与 validation runtime artifacts 仅在 final commit 上重建并保留；
- recovery：合入前由 task branch 恢复，合入后由 local/remote main 恢复；已登记的
  `docs/research/growth_tilt_owner_diagnosis_pack.md` 无关改动不得读取、hash、stage 或修改。

## 7. 进度记录

- 2026-08-23：Project Owner 要求 Codex 继续完善；只读复核确认现有 2482/2534 authority 正确保持
  numeric thresholds unknown。2542B 进入 registration，未运行 DQ/provider/cache/backtest/empirical/
  external/trading action。
- 2026-08-23：SINGLE_LANE START/LANE preflight PASS；新增 strict typed YAML loader、canonical JSON
  seal/replay、root-bound/symlink-closed cross-file identity validation，以及 contract/session/window pure
  evaluator。真实 evidence 在 numeric authority 未审批时机械返回 `AUTHORITY_UNAVAILABLE`；只有 exact
  `SYNTHETIC_CONTRACT_VALIDATION_ONLY` 才能测试 intent boundaries，且 synthetic thresholds 用于真实
  evidence 会返回 `INVALID`。
- 2026-08-23：contract file SHA-256=`a60b6c71e492aacac31d8fc9a4f4d406659679c6a1f88ac9e53664d49134d138`，
  canonical SHA-256=`d7c6bfe8fcb8123be6b8d6f87c5ba72a90db3c5ac50af041d1b3f5eefcc32f68`；
  focused parallel pytest=`36 passed`，Ruff 与 strict mypy=`PASS`。V2 file/canonical identity 仍为
  `bbb2e0ade108213269c3c9524b465836518457d932a6344887e6d8afb89ae620` /
  `b978e952c4767756025fc01b17f8694004e720a5bb44aa5dde893628a4d9c199`，字节未修改。
- 2026-08-23：治理同步 focused=`85 passed`；正式 Architecture=`865 passed`、Contract=`276 passed`、
  Integration=`995 passed / 643 warnings`、Reproducibility=`24 passed`。runtime artifacts 分别为
  `outputs/validation_runtime/architecture-fitness_20260823T101117Z/`、
  `contract-validation_20260823T101759Z/`、`integration_20260823T102040Z/` 与
  `reproducibility_20260823T102139Z/`。006D lossless shadow=`2999 entries / 192 fragments`，006C、
  ARCH-004E 和 canonical task registry 均 PASS；剩余工程门为 governed integration preflight 与独占
  Full final-tree validation。
