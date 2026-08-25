# TRADING-2542F：Growth Action Value Veto / Option Signal 架构合同 V1

最后更新：2026-08-25

稳定任务 ID：`TRADING-2542F_GROWTH_ACTION_VALUE_VETO_OPTION_SIGNAL_ARCHITECTURE_CONTRACT_V1`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`（合同草案已实现并验证；等待 Owner exact-freeze）

governed mode：`SINGLE_LANE` serial contract wave

`production_effect=none`；`broker_action=none`；`external_action=none`。

## 1. Owner 决定与范围

2026-08-25，Project Owner 在 exact-commit Web Pro 规划复核完成后明确要求：
“基于这个方案继续推进吧”。本任务把该指令固定为：

- 授权实现一个 result-blind、non-executable 的 veto / option-signal serial contract wave；
- 保持既有 V1/V2、DQ/PIT V3 与 exact sheet V4 bytes、hash 和历史语义不可变；
- 只允许新增 architecture contract、legacy compatibility map、typed loader/validator、
  synthetic-only negative tests 与 `docs/system_flow.md` 合同视图；
- 不授权真实 veto series、R1 manifest、provider/cache/QuantConnect 访问、真实 DQ、backtest、
  threshold selection、candidate search、raw option row/SID 导出、paper/live、production 或 broker action；
- 新合同完成后仍须由 Owner exact-freeze，才能进入独立 source-contract wave。

Owner decision id：
`owner_decision:TRADING-2542F:2026-08-25:authorize_serial_veto_option_architecture_contract_wave_v1`。

## 2. 问题与目标

TRADING-2542E 已证明 primary-window option transport 为 exact `1202/1202`，但该证据只证明
transport completeness，不等于完整 option surface 或 DQ/PIT admission。当前 frozen five-veto
taxonomy 与实际 producer 存在以下不可原地修补的语义冲突：

- `risk_off_veto` 当前由 `growth_allowed == false` 反向构造，不是独立 market-state source；
- `event_risk_veto` 没有 PIT-approved producer 和 `available_at` contract；
- `trend_break_veto` 没有 callable veto producer；
- `tqqq_veto` 在实际 producer 中恒为 `true`，语义是 no-TQQQ action guard，与 QQQ/SGOV-only
  action universe 一致，但与 all-false market-veto gate 不兼容；
- 当前只有 `volatility_veto` 具备 compatible source contract；
- 早期 option surface evidence 只有 `182/1202` session PRESENT，`1020` session
  MISSING/NOT_EVALUATED，且 `182` 个 underlying/cross-field consistency 为 INVALID。

本任务目标是先冻结 consumer-visible architecture boundary，使后续研究不需要弱化旧 policy、
把 missing 填成 false，或让同一个 selected CALL/PUT activity 同时承担 alpha 与 veto。

## 3. 不可变上游 authority

- frozen execution V1：
  `config/research/qc_qqq_options_growth_action_value_real_review_execution_v1.yaml`；
  file/canonical SHA-256=
  `03edd3868da276be69652cd9854f0201934a6cf2fa4eb5c40bfcfb4ff06206c1` /
  `7daa5d1cb212051aab34d2af6477ee410ea8492f62c74da7417174ec3586e717`；
- immutable successor V2：
  `config/research/qc_qqq_options_growth_action_value_real_review_execution_v2.yaml`；
  file/canonical SHA-256=
  `f02df23a4bd36069f5fe09354a3ce8480583fc451b71ec511bc3ba2da27780f2` /
  `9b39a1cf6d1ad48c427755f07c592610ae2ad94055af4aab79d3327bf4e82456`；
- DQ/PIT V3：
  `config/research/strategy_growth_action_value_canonical_dq_pit_contract_v3.yaml`；
  file/canonical SHA-256=
  `96eafe7525704a8e0e260c9ed344adf3420f7e1c977e877a557856258fee3144` /
  `e8e180b147e1a88dad3776f886b8eb7398481b1518785b6a2243ae795f4a6ede`；
- exact sheet V4：
  `config/research/strategy_growth_action_value_threshold_exact_value_sheet_v4.yaml`；
  file/canonical SHA-256=
  `c90c4cc22b8918e90641bf0553416a68458433bea750bd2064fcf98df7886215` /
  `00198bb84cd57f518d0370035b5a5a38b12c9804880d7bf1e475ddd80a77bfc2`；
- primary requested/evaluated range：`2021-02-22..2025-12-02`，exact `1202` sessions；
- first-target prior：独立 `2021-02-19`，不得扩入 target inventory；
- action universe：仅 `QQQ/SGOV`，无 options position、TQQQ/QLD 或 borrowed leverage。

本任务不得修改、重写或重新 canonicalize 上述四项 authority。

## 4. 推荐分层与消费合同

新合同 artifact 使用 V1 schema/version，但声明
`policy_family_generation=RESULT_BLIND_SUCCESSOR_V3`，避免与独立的 DQ/PIT V3 混淆。

1. `L0_AUTHORITY_AND_IDENTITY`
   - exact policy/source/code/calendar/inventory identity；
   - decision-as-of、source session、`available_at` 与 next-session effective mapping。
2. `L1_DATA_QUALIFICATION`
   - terminal=`PASS|FAIL|INSUFFICIENT|INVALID`；
   - `INVALID > FAIL > INSUFFICIENT > PASS`；
   - 非 PASS 不得转换成 market-clear boolean。
3. `L2_ACTION_UNIVERSE_CONSTRAINTS`
   - QQQ/SGOV-only；no options position；no TQQQ/QLD；no borrowed leverage；
   - historical `tqqq_veto` 映射为 `NO_LEVERAGE_ETF_ACTION_GUARD`。
4. `L3_ORTHOGONAL_MARKET_STATE_VETOES`
   - `broad_market_risk_off_veto`；
   - `realized_volatility_veto`；
   - `scheduled_event_risk_veto`；
   - `underlying_trend_break_veto`。
5. `L4_OPTION_ALPHA_SIGNAL`
   - frozen selected CALL/PUT activity comparison 只负责 alpha state；
   - 不得成为任何 mandatory veto 的输入。
6. `L5_OPTION_RISK_DIAGNOSTICS`
   - optional、independent、pre-selection、derived-only；
   - minimum successor 中不得作为 mandatory market-clear gate。
7. `L6_NEXT_SESSION_ACTION_JOIN`
   - 仅在 DQ=`PASS`、action guard=`PASS`、四个 mandatory market veto 全部 exact false 后，
     才允许消费 option alpha；
   - 本任务只冻结 join contract，不生成 weight、return、series 或 manifest。

## 5. Legacy compatibility map

| legacy field | successor role | 本任务结论 |
| --- | --- | --- |
| `risk_off_veto` | `broad_market_risk_off_veto` | 必须使用独立 producer；禁止 `growth_allowed` 反向 alias |
| `volatility_veto` | `realized_volatility_veto` | 保留 compatible source role；不在本任务生成 series |
| `event_risk_veto` | `scheduled_event_risk_veto` | 日历 veto 与未来 option-event-premium diagnostic 分离 |
| `trend_break_veto` | `underlying_trend_break_veto` | 保持 QQQ underlying 正交，不强行期权化 |
| `tqqq_veto` | `NO_LEVERAGE_ETF_ACTION_GUARD` | 移出 market-state all-false gate；历史字段只读保留 |

legacy five-veto bytes 和 labels 继续用于 immutable replay；新 consumer 不得隐式消费旧字段，
也不得裁切 retained labels 或把 missing/unknown/non-PIT 填成 false。

## 6. Alpha / veto dependency ban

任一 mandatory market veto 或 mandatory option-risk gate 必须拒绝以下输入：

- selected CALL/PUT contract identity、selected-pair checksum；
- selected call/put activity；
- `growth_active`、`growth_inactive`；
- candidate weights、returns 或任一 V4 result；
- result-dependent/adaptive contributor universe。

未来若 Owner 单独批准 option-risk aggregate，只允许使用结果前固定的 pre-selection universe、
fixed right/DTE/moneyness buckets、独立 lineage/cache key、contributor checksum 与 raw-export=0。
当前 collector capacity、daily quote-age 缺口和 182-session negative capability evidence 都必须显式
保留，不能推断已经具备完整 IV/skew/term surface。

## 7. 分阶段实施

### S0：任务登记与合同草案

- canonical task registration 与本 requirement；
- 新 architecture config 与 legacy compatibility config；
- 严格 typed loader/validator；
- synthetic-only dependency、terminal、immutability 与 safety negative tests；
- 更新 `docs/system_flow.md`。

### S1：Owner exact-freeze

- 展示新合同 file/canonical SHA；
- Owner 逐项复核 taxonomy、layer semantics、tqqq migration、event split、dependency ban、
  terminal 与 compatibility roles；
- 未 exact-freeze 时任务终态只能为
  `DRAFT_READY_FOR_OWNER_EXACT_FREEZE_NO_EXECUTION_AUTHORITY`。

### S2：后续独立 source-contract wave（本任务之外）

- 分别冻结 risk-off、volatility、scheduled-event、underlying-trend producer、formula category、
  `available_at`、missing terminal 与 exact 1202 identity；
- thresholds/formulas 必须由 Owner result-blind exact-freeze；
- 任一 mandatory source 不完整时停止于 R1 manifest 之前。

### S3：可选 option-derived data lane（本任务之外）

- 独立授权新的 versioned derived-only collector；
- 禁止复用 selected pair、raw rows、SID 或结果后 buckets；
- 完整 IV/skew/term surface 保持 deferred，除非以后取得 exact 1202 PIT-valid evidence。

## 8. Path、module、contract 与 evidence-lineage claims

task-owned paths：

- `docs/requirements/TRADING-2542F_Growth_Action_Value_Veto_Option_Signal_Architecture_Contract_V1.md`；
- `config/research/qc_qqq_options_growth_action_value_veto_option_signal_architecture_v1.yaml`；
- `config/research/qc_qqq_options_growth_action_value_legacy_veto_compatibility_map_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/growth_action_value_veto_option_signal_architecture.py`；
- `tests/test_growth_action_value_veto_option_signal_architecture.py`。

coordinator-owned paths：canonical task fragment/index、generated task views、
`docs/system_flow.md` 及适用的 architecture/report/compatibility generated authority。

evidence roles：

- TRADING-2541 terminal evidence=`TRANSPORT_COMPLETENESS_LINEAGE_ONLY`；
- TRADING-2530 aggregate evidence=`NEGATIVE_CAPABILITY_EVIDENCE_ONLY`；
- V1/V2=`IMMUTABLE_LEGACY_POLICY_AUTHORITY`；
- DQ/PIT V3=`UNCHANGED_DATA_QUALIFICATION_AUTHORITY`；
- exact sheet V4=`UNCHANGED_FUTURE_EVALUATION_AUTHORITY`；
- capability graph/actual producer=`SOURCE_READINESS_EVIDENCE_ONLY`。

## 9. 验收与 stop conditions

- V1/V2/DQ-PIT-V3/exact-sheet-V4 exact file/canonical hashes保持不变；
- legacy five-veto 可 replay，但新 consumer 不得直接消费；
- DQ terminal 与 market-veto boolean 完全分层；
- `tqqq_veto` 只映射为 action guard，不进入 all-market-clear gate；
- risk-off 不得依赖 `growth_allowed`；event calendar 与 option event premium 分离；
- trend break 只消费 independent QQQ underlying state；
- schema 机械拒绝 selected-pair/alpha/result 字段进入 veto；
- mandatory gate 缺 source SHA、producer、`available_at`、missing terminal 或 exact 1202 inventory 时 typed block；
- no constant false fill、no retained-series truncation、no cross-date fallback；
- manifest/provider/cache/real-DQ/backtest/production/broker flags 全部 false；
- focused、Ruff、strict mypy、Architecture、Contract、Integration、Reproducibility 与适用 Full PASS；
- Owner exact-freeze 前不得进入任何 execution-authority 状态。

最早 fail-closed 点固定为：任何 mandatory gate 缺少 exact source-contract SHA、独立 producer、
`available_at`、missing terminal 或 exact 1202 inventory 时，在 R1 manifest 生成之前停止。

## 10. 生命周期

- registration branch：`codex/trading-2542f-register`；
- implementation branch：`codex/trading-2542f-veto-option-architecture`；
- repository workspace：复用 `D:\Work\AITradingSystem`，不创建额外 worktree/clone/cache；
- registration commits 作为正式 implementation lane 的祖先保留；不单独发布中间 registration candidate；
- 最终实现候选只发布一次，并在 closeout 后清理 task branch；
- known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、diff、stage 或修改；
- recovery：所有 tracked bytes 由 Git/main 恢复；没有外部 dataset、provider artifact 或 broker state；
- exit condition：合同草案验证、普通发布与 task 状态更新完成；Owner exact-freeze 前保持 non-executable。

## 11. 进度记录

- 2026-08-25：Owner 授权按 Web Pro exact-commit 复核方案继续推进。READ_ONLY preflight PASS：
  `main=origin/main=5996affa2a458e3792cc1c06d6c383f8e7ad1298`、active lease=0、
  worktree audit PASS。选择 `SINGLE_LANE` serial contract wave；未生成 veto series/R1 manifest，
  未读取 provider/cache，未运行真实 DQ/backtest，orders/fills/positions/production/broker=0。
- 2026-08-25：registration transaction v1 完成 canonical task registration 后，self-hosted
  task-source focused test 机械发现 task count 仍冻结为 `1026`、实际为 `1027`；v1 以 FAIL 释放，
  未发布、未绕过测试。v2 精确扩展 `tests/test_arch_005_s5_task_source_cutover.py` path，只同步该
  canonical registry invariant，不改变任何策略、DQ/PIT、数据或执行语义。
- 2026-08-25：registration transaction v2 的 task-source focused test=`9 passed`。为避免中间
  registration candidate 与最终实现各运行一次完整 Full，v2 未发布并以 FAIL 释放；已提交登记 bytes
  直接作为 `codex/trading-2542f-veto-option-architecture` 的祖先，然后 SINGLE_LANE LANE preflight
  PASS。该收敛不跳过最终候选的正式 validation、publication fence、local-main fast-forward 或普通 push。
- 2026-08-25：result-blind architecture 与 legacy compatibility map 已实现。architecture
  file/canonical SHA-256=`9b4856614298d64b2c8b5897980735a9e2a19c46fecb6c2362cb750ae13b136d` /
  `88e1283b0333bafca24779c9c527d362acef40b65d4cff1a9d081ded07ac70e4`；compatibility map
  file/canonical SHA-256=`c5867551aec4f152256219e4fb19b7c52ec5a6b7f8d8c316961d33a75749679d` /
  `067a6b23daa1bfff22a6d4f4fcb773346a7d866e21cf2adb759acde75d04f524`。focused/adjacent=
  `66 passed`，Ruff、strict mypy、py_compile PASS。terminal=
  `DRAFT_READY_FOR_OWNER_EXACT_FREEZE_NO_EXECUTION_AUTHORITY`；veto series/R1 manifest/provider/cache/
  real DQ/backtest 均未运行，orders/fills/positions/production/broker=0。
- 2026-08-25：publication transaction v1 在任何 official generator 或 Full 前发现
  `tests/atlas/test_historical_projection_review.py` 也冻结了 2542E 后继 coverage 顺序，但该路径未在
  v1 shared claim 中声明；v1 按 scope-expansion fail closed 并释放。v2 一次性加入该精确 consumer，
  继续绑定同一 frozen base/lane head；不修改 architecture 数值或放宽执行边界。
- 2026-08-25：publication transaction v2 在 `GENERATED_REBUILD_POST` 后的提交前语义审阅中发现
  authority loader 的相对路径仍需显式拒绝 `..` traversal。v2 未在 post-generator 状态继续写入，按
  fail-closed 规则释放；v3 复用相同 frozen base/lane head 与精确 path claims，直接修复 root containment、
  增加 typed rejection 回归测试并从头 replay generator/validation。该修复不改变 veto taxonomy、阈值、
  DQ/PIT、数据权限或执行边界。
