# TRADING-2520 — QQQ Options 主窗口 daily Slice 零订单再验证 V1

- status: `BASELINE_DONE`
- priority: `P0`
- governed mode: `SINGLE_LANE`
- registration base: `8f1b8c3fc1c4815fc0041569a1bce01634908229`
- predecessor: `TRADING-2519_QC_QQQ_OPTIONS_PRIMARY_WINDOW_DAILY_SCHEDULE_RESULT_FAILURE_ADMISSION_V1`
- production effect: `none`
- external action: `none`

## 背景与问题陈述

TRADING-2519 已将 QuantConnect 主窗口运行
`b6d711f67a47199667c8a62f86208b28` 封存为 `INVALID_INCOMPLETE`：请求和评估区间均为
`2021-02-22..2025-12-02`，运行处理 `38,397,482` 个数据点且保持零订单、零成交，
但导出的 derived aggregate 显示 `0/1202` 个有效 session。失败运行结果文件 SHA-256 为
`30f95852fe509e5229a86bed77978f62f9756016f17c3159c5afb63b6eaa205b`。

当前主要、但尚未由 Cloud 证据证明的假设是：原收集器在每日 `09:31` scheduled callback
读取 option chain，而 `Resolution.DAILY` option chain 的 Slice 尚未在该时点交付。2519 已准备
改为在 `on_data(self, data: Slice)` 中读取 `data.option_chains` 的 successor code，LF SHA-256 为
`d5d8638a2e864b5182887da11d0d74a181dec2e7be41f40bc709f2e245a35261`。

离线审计已经确认一个独立于调度时点的真实代码缺陷：2519 successor 读取
`contract.underlying`，但 LEAN `OptionContract` 的公开 underlying price authority 是
`UnderlyingLastPrice`，Python accessor 为 `underlying_last_price`。旧测试 fixture 恰好自造了
`underlying` 属性，因而掩盖了真实 runtime API mismatch。2520 将该 accessor 修正为
`underlying_last_price`；scheduled callback 与 daily Slice delivery 的因果关系仍保持
`UNVERIFIED_PRIMARY_HYPOTHESIS`，不能仅凭离线代码审计宣称已经证实。

本任务不会把该假设当作已证实根因，也不会把修复代码当作已经获得数据能力。任务目标是完成
离线、可审计、可证伪的再验证包，并排除 code path 内仍可能导致全量 session 被拒绝的属性解析、
会话身份和 aggregate 完整性问题。

## 冻结边界

### 允许的离线工作

- 审计 2519 successor `main.py` 对 daily Slice、option chain、quote、Greeks、IV、OI、volume、
  underlying price 和 session identity 的解析；
- 形成版本化 policy、proposal、run scope、project code、package manifest、typed admission 和测试；
- 使用纯离线 fixtures 做 unit/property/golden/negative validation；
- 生成一个尚未签署的 Owner authorization template，并公布其 exact hashes。

### 未授权工作

在 Project Owner 提供绑定本任务最终 exact main、policy/package/code hashes 的新单次 token 之前，
以下动作全部禁止：

- QuantConnect login、project read/write、Cloud build/backtest、Results 收集；
- API、CLI、HTTP、Object Store、raw options data 下载、记录或导出；
- 第二项目、范围扩展、订单、成交、paper/live/broker/production；
- 将零 slippage、任意 fill 或任意阈值当作现实基线；
- 对投资表现、策略有效性或生产可用性作结论。

2518 v3 token 已在首次 run attempt 后失效，不能复用。

## 可证伪假设

1. `H1_DAILY_SLICE_DELIVERY`：在 `on_data` 中读取 canonical option-chain key 可获得主窗口
   daily chain；若再次为零，则该假设被否证或仍有 subscription/filter 问题。
2. `H2_ATTRIBUTE_TRANSPORT`：quote、Greeks、IV、OI、volume、underlying 的 transport 形态可由
   明确、有限、可测试的 canonical accessor 解析；任何缺失或非有限值必须使该 session fail closed。
3. `H3_SESSION_IDENTITY`：只接受 reviewed exchange calendar 的 1202 个 expected sessions；重复、
   越界、未观察或非交易日不得被静默计入。
4. `H4_AGGREGATE_COMPLETENESS`：只有全部 required derived series 对全部 1202 sessions 完整、有限且
   身份一致时，候选结果才可进入 `GO_FOR_DAILY_ENGINEERING_ONLY` 的后续 admission review。

Accessor 结论以 QuantConnect/LEAN primary authority 为依据：AlgoSeek US Equity Options、Time Slice、
Initialization 文档，以及 LEAN `Common/Data/Market/OptionContract.cs`。其中只有
`H2_OPTION_CONTRACT_UNDERLYING_ACCESSOR` 被标记为 `CONFIRMED_OFFLINE_CODE_DEFECT`；daily delivery、
time frontier 和全窗口 transport/coverage 仍必须由新的 bounded zero-order Cloud 结果证伪或确认。

## 再验证运行上限（仅供未来新 token 绑定）

- target project id: `34808569`
- requested/evaluated range: `2021-02-22..2025-12-02`
- expected session count: `1202`
- maximum project mutations: `1`
- maximum Cloud backtests: `1`
- maximum orders: `0`
- maximum fills: `0`
- raw option rows logged/exported: `0`

这些是 fail-closed 上限，不是当前授权。

## Admission 语义

候选 `PASS` 必须同时满足：

- exact project-code、policy、package、range、calendar 和 expected-session identity；
- `observed_sessions=1202`、`invalid_sessions=0`；
- 每个 required derived aggregate series 恰好覆盖 1202 个 session，日期顺序和 checksum 一致；
- 所有值均为有限数值，且每个 session 的 positive OI 约束成立；
- `orders=0`、`fills=0`；
- 无 raw option rows、无 prohibited transport/action；
- evidence、DQ/PIT 和 option-event DQ 状态由真实 result facts 派生，绝不由调用者自报。

否则必须输出 typed failure，至少覆盖：

- `DAILY_SLICE_OPTION_CHAIN_NOT_DELIVERED`
- `SESSION_COVERAGE_INCOMPLETE`
- `DERIVED_SERIES_MISSING_OR_MALFORMED`
- `PROJECT_CODE_IDENTITY_MISMATCH`
- `RANGE_OR_SESSION_IDENTITY_MISMATCH`
- `ORDER_OR_FILL_PROHIBITION_BREACH`
- `RAW_OR_PROHIBITED_TRANSPORT_BREACH`

任何 `UNKNOWN` 不得升级为 `PASS`。在真实再验证成功且完成独立 Owner review 前：

- evidence status = `FAIL` 或 `NOT_EVALUATED`
- DQ/PIT = `NOT_EVALUATED`
- option-event DQ/PIT = `NOT_EVALUATED`
- engine decision = `POLICY_BLOCKED_CASH_PRESERVATION`

## 实施步骤

1. 登记本任务并发布短 registration boundary。
2. 从该 exact main 创建 SINGLE_LANE task branch，完成 transport/accessor 和 session identity 离线审计。
3. 实现严格的 policy、typed builder/admission、project package 与 authorization template。
4. 增加 unit/property/golden/negative tests，覆盖输入排列不变、缺失/重复 session、malformed series、
   forged PASS、order/fill/raw transport breach 和 identity mismatch。
5. 重建 shared generated/compatibility authority，运行 focused 与正式五级门禁。
6. ordinary non-force push、SHA verify、branch/worktree cleanup；发布供 Owner 选择是否授权的 exact hashes。

## 验收标准

- 不修改或重定义 2481–2519 shared contract、DQ/PIT、adapter、selector 或 evidence admission 语义；
- 不引入投资解释阈值，也不把未验证假设写成事实；
- offline package inventory、canonical bytes、content hashes 和 strict loader fail closed；
- 测试证明所有不完整、伪造、越界和 prohibited 情形保持 cash preservation；
- tracked task row、supporting requirement、system flow、generated/task shadow/compat authority 一致；
- final-tree formal gates PASS 后才可发布新的 token template；
- 本任务完成本身不执行任何外部动作。

## 冻结离线身份

- revalidation policy file SHA-256：
  `f9f859568e34c836a2453b175dc283cbdeec7a009887f6f868beccaabd14f35c`；
- revalidation policy canonical SHA-256：
  `fc665f68e9fc6bbf52fdb0a3bc903aca13800cb2acdc22d5dd8bd0acd81588b3`；
- package manifest file/content SHA-256：
  `c6d632c0813b47d3a4e96a98457a43403387b79c6c90e214bd9fe1ddb66ee605`；
- investigation content SHA-256：
  `5ff1e87f1b0c43bba11b72ebdd61a93097669821961e2423dde3666343a00fba`；
- proposal content SHA-256：
  `d17db4d8944483f6066011c5a854600ea2fdac4a23e91e8b869870c6795e85bb`；
- run-scope content SHA-256：
  `7d20c370edfb7653da799444d08b9ceb713c33072f33e4eb3e1f2b7535fbfb14`；
- corrected project code LF SHA-256：
  `88a60874737c1e210f5a2f5ac990d14d0f4de3024a1db8f41edaddf3db6226aa`。

`owner_decision_request.md` 中的 manifest/main placeholders 有意保持未填：把 manifest 自身 SHA 写入其
inventory 中的模板会形成循环身份。ordinary-pushed main 与上述 package manifest SHA 应由最终交接消息
共同绑定，Owner 不能仅复制未签署模板而跳过 exact-main 复核。

## 进度记录

- `2026-08-14T15:13:43Z`：从 clean exact main
  `8f1b8c3fc1c4815fc0041569a1bce01634908229` 开始登记；external action 保持 `none`。
- `2026-08-14T15:18:00Z`：registration boundary 已 ordinary push，exact main=
  `54e43a1aa9787c52d4b0cb363e30e5a4bf79aed9`；从该基线 START/LANE PASS。
- `2026-08-14T15:26:00Z`：离线 primary-source/API 审计确认 2519 的
  `contract.underlying` accessor 缺陷；新 code 使用 `underlying_last_price`、
  `daily_precise_end_time=true`、`data.time.date()` 与 export-safe diagnostic counts。
- `2026-08-14T15:29:00Z`：首轮 adjacent focused coverage 为 `80 passed / 2 failed`；两个失败均为
  canonical package owner template 在 formatter 后陈旧，不是 runtime semantics。使用同一 canonical writer
  重建 package 后，完全相同的 `-n 16 --dist loadfile` 覆盖为 `82 passed`。
- `2026-08-14T15:44:00Z`：Atlas/page-effectiveness 首轮为 `20 passed / 1 failed`，唯一失败是
  renderer test 仍冻结 38-task count；同步为 39 后完全相同的 `-n 16 --dist loadfile` 覆盖为
  `21 passed`。页面新增 2520 disclosure，但不改变人工验收轨道或策略结论。
- `2026-08-14T15:47:00Z`：task projection 更新为 `BASELINE_DONE`。其含义仅为离线根因、严格包、
  修正版 project code、tests 与 Owner request template 已形成；不表示 Cloud 再验证、evidence/DQ-PIT PASS、
  selection 或 engine activation。
- `2026-08-14T16:12:00Z`：compatibility/deprecation 首次原样运行在外层 304 秒上限被终止，无 pytest
  terminal/node failure，不能作证据；放宽 wrapper 等待时间后的相同 211 项覆盖为 `210 passed / 1 failed`，
  暴露旧 ARCH-004G inventory id。仅同步测试 current-authority 常量后第二轮仍为 `210/1`，进一步确认 frozen
  inventory 文件本身陈旧。最终按 `PYTHONPATH=src;.` 的真实 inventory 更新 id 与 module/test counts，第三轮
  完全相同的 `-n 16 --dist loadfile` 覆盖为 `211 passed`；历史 prefix、exact bytes、source hash 与 removal
  safety 验证均未放宽。
