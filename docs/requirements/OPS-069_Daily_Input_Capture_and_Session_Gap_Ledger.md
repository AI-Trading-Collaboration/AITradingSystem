# OPS-069 Daily Input Capture and Session Gap Ledger

## 状态

- task id：`OPS-069_DAILY_INPUT_CAPTURE_AND_SESSION_GAP_LEDGER`
- priority：P0
- status：`IN_PROGRESS`
- owner：operations owner + data platform owner
- last updated：2026-07-25

## 问题与根因

`aits ops daily-run` 目前采用单链 fail-closed 执行。该安全边界必须保留，但它把“是否允许
下游消费”与“是否尽量保全当日可获得输入”绑定在了一起：

1. 全计划环境变量预检查会在第一条命令前阻断，因此一个后段 provider 的密钥缺失会让其他
   可用来源也完全不抓取；
2. FMP PIT、SEC companyfacts、valuation、官方政策来源位于严格 DQ 之后，市场 DQ 失败时
   这些当日输入不会留下可审计副本；
3. 多个 live provider 步骤串行执行，任一失败会阻止其后的独立来源；
4. scheduler 只解析最新已完成交易日，缺少逐 XNYS trading session 的持续缺口台账，因而
   中间漏跑日期可能只存在于分散的 run state/ledger 中。

这不是放宽 DQ、PIT 或评分门禁的理由。目标是把输入保全与消费授权分离：尽量保全、逐源
记账，但只有所有必需 capture component 通过，且后续既有 DQ/PIT/score gates 全部通过，
才允许继续日报链。

## Owner 选择与治理边界

- 继续采用 D0B2B 已批准的方案 A：`XNYS decision-session aligned`。
- `aits ops daily-run` 仍是唯一外部 scheduler trigger。
- 新增一个内部 `capture_daily_inputs` umbrella step；它必须逐一尝试所有启用来源，即使
  前一来源失败也不得停止其余来源。
- umbrella step 在任一 required component 失败时整体返回非零，因而严格阻断
  `validate-data -> PIT -> score -> Reader Brief` 消费链；`download-data` 是 umbrella
  内的 required capture component，不再因其他 provider 先失败而漏跑。
- 不允许以 capture 成功替代 `aits validate-data`、PIT validation、SEC validation、
  valuation validation、score/finalization validation。
- capture artifact 固定
  `production_effect=none`、`production_weight_write=false`、
  `active_shadow_weight_write=false`、`broker_action=false`、
  `trading_action=false`。
- 不补抓或回填缺失日期为 strict PIT；历史缺口只能登记 `MISSED`、`PARTIAL_CAPTURE`
  或 `INSUFFICIENT_DATA`。

## 实现步骤

### S1：受治理 policy 与 artifact contract

新增 reviewed policy，定义：

- tracking start；
- required components；
- date-scoped raw/processed/external roots；
- manifest 与 session gap ledger schema/status；
- retention 只证明 bytes 已保全，不证明可消费；
- 每项 artifact 的相对路径、SHA-256、bytes、captured_at 和 provider command provenance。

### S2：Capture-first orchestration

新增 `aits ops capture-daily-inputs --as-of YYYY-MM-DD`，至少覆盖：

- market/macro canonical download，并保留最多两次受控尝试及同日 CSV/manifest 快照；
- FMP forward PIT；
- SEC companyfacts；
- FMP valuation / analyst estimates；
- official policy/geopolitical sources。

命令必须捕获并脱敏每个 component 的 return code、stdout/stderr 行数与错误摘要，最后统一
写 manifest/Markdown。不得把 API key、Authorization header、未授权付费内容或完整响应
写入汇总 artifact。

### S3：Consumer binding

- PIT build/validate 从同日 capture paths 读取；
- SEC metrics 从同日 SEC capture path 读取；
- score-daily 从同日 valuation capture path 读取；
- daily plan 不再重复执行已由 umbrella step 完成的 live provider fetch；
- trading-day `download-data` 纳入 umbrella、继续使用 canonical atomic publication，
  成功后把四个 canonical CSV/manifest 复制到同日 capture path；strict DQ 紧随 capture，
  仍只读取 canonical cache，质量与消费授权语义不变；
- closed-market 条件刷新仍保留独立 `download-data` 计划项。

### S4：Session gap ledger

从 reviewed `tracking_start` 到 `as_of` 按项目 XNYS calendar authority 枚举每个 trading
session，状态至少为：

- `CAPTURED`：required components 全部成功，manifest 校验通过；
- `PARTIAL_CAPTURE`：至少一个 component/expected artifact 失败；
- `MISSED`：该 trading session 没有 manifest；
- `INSUFFICIENT_DATA`：policy 或历史证据不足，不能补造结论。

ledger 必须能够在“最新日期运行成功但中间日期漏跑”的情况下保持中间 `MISSED` 可见。

### S5：验证与运营验收

工程 acceptance：

- focused tests 证明单 component 失败后其余 component 仍被尝试；
- market/macro component 保留两次受控尝试，成功 canonical files 的同日快照 checksum
  可校验；
- partial manifest 与 gap ledger 即使 umbrella 最终失败也落盘；
- manifest checksum tamper fail closed；
- daily plan 的 capture 位于任何 strict consumer 之前；
- DQ/PIT/score gate 语义与 no-production safety 不变；
- architecture、contract、integration、reproducibility 与 Full tier 通过。

运营 acceptance：

- 在下一个合法 provider-ready XNYS trading date，仅通过新的
  `aits ops daily-run` 形成真实 capture evidence；
- 若 provider 失败，必须看到 `PARTIAL_CAPTURE` 及成功来源的 retained artifacts；
- 若全部成功，后续 strict DQ/PIT/score/Reader Brief 仍须独立通过；
- 不重试旧 terminal key，不改写既有 FAILED state/ledger。

## Blocker 与退出条件

当前 blocker 是尚无新合法 provider-ready trading date 的真实运行证据。工程验证完成后，
任务保持 `IN_PROGRESS`，直到真实 run 证明“部分来源故障不再造成其他来源零捕获”，且
gap ledger 能连续覆盖自 tracking start 起的每个 XNYS session。
