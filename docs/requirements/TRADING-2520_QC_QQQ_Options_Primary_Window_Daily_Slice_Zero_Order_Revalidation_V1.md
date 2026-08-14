# TRADING-2520 — QQQ Options 主窗口 daily Slice 零订单再验证 V1

- status: `IN_PROGRESS`
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

## 进度记录

- `2026-08-14T15:13:43Z`：从 clean exact main
  `8f1b8c3fc1c4815fc0041569a1bce01634908229` 开始登记；external action 保持 `none`。
