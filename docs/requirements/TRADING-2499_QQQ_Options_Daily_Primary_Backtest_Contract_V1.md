# TRADING-2499：QQQ Options Daily Primary Backtest Contract V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标与授权边界

TRADING-2500 的 independent reviewer 已接受 exact canonical evidence，并将 terminal gate 冻结为
`GO_FOR_DAILY_ENGINEERING_ONLY`。该 GO 只授权建立 QQQ Options 日级 primary backtest 的离线工程合同；
它不授权新的 QuantConnect 登录、project mutation、Cloud backtest、API/CLI/HTTP/Object Store、raw data
download/export、purchase/subscription、paper/live/broker/production，也不构成历史全窗稳定性、策略收益或
投资价值证明。

本任务必须消费 2500 strict review loader 的事实，不得从聊天文本、自报 PASS 或未校验 bytes 直接晋级。
2493 的 broader `NO_GO_KEEP_BLOCKED` 仅在 2500 精确批准的 DAILY engineering scope 内被窄化；2489/2490、
license/download 和完整外部历史回测继续 blocked。

## 2. 冻结继承

- 2481：只使用 shared `OrderIntentRecord`、`OrderEventRecord`、`FillEventRecord`、DQ envelope/public enum
  的 canonical seal/replay authority，不复制 shared records；
- 2482：15 个 DQ/PIT checks、typed reason taxonomy、calendar/session、cache/evidence identity；`UNKNOWN`
  永不产生 PASS；
- 2483：strict signal package layout、canonical DQ report/receipt fact derivation、PRIMARY window；
- 2484：QC adapter package identity与 result mapping contract；日级 contract 不得静默改写既有 minute adapter；
- 2485：selector 仍为 `OWNER_REVIEW_REQUIRED_BASELINE`、`selection_authorized=false`；
- 2486：execution reality model 的 event identity/cash preservation；不得把 minute 数值或 zero-slippage isolation
  sensitivity 偷换成日级 reality baseline；
- 2487/2488：accounting/lifecycle mechanics 可复用，但 policy-dependent 路径继续 blocked；
- 2493/2497：broader platform stage gate 与 license/export/download boundary；
- 2500：reviewed Free Cloud DAILY capability facts与 `DAILY_ENGINEERING_ONLY` successor scope。

## 3. Primary research window 与日级 chronology

新 primary run 的默认 requested/evaluated start 必须为 `2021-02-22`，并在 plan、manifest、result 中同时披露
requested 与实际 evaluated range。其他 start 仅可作为 reviewed sensitivity/proxy/stress role，并带 DQ caveat；
`2022-12-01` 绝不是默认值。

合同至少区分 `signal_session`、`selection_session`、`intent_session`、`submit_session`、`fill_session` 与
`valuation_session`。禁止：

- 同一 session 的 close 既生成 signal 又用于 selection/fill；
- daily-close 或 same-bar fill；
- 当日 OI、Greeks、IV 在其可得性前用于当日 selection；
- 用缺失/stale/single-sided quote 推断成交；
- 因 selection-stage DQ PASS 伪造全生命周期 DQ PASS。

默认 chronology 必须 fail closed 地要求独立、已完成 session；prior-session OI/Greeks/IV 必须绑定 reviewed
XNYS calendar。尚未评估的 execution/accounting/lifecycle checks 保持 `NOT_EVALUATED` 或 typed FAIL。

## 4. Contract outputs

首个 contract wave 只产生 deterministic、sealed、canonical 的 offline plan/descriptor/admission result，至少绑定：

- repository code SHA、2500 review file/content/evidence/result hashes；
- shared contract/policy、DQ/PIT、signal package、adapter、selector、execution、accounting、lifecycle hashes；
- ticker=`QQQ`、equity/options resolution=`DAILY`、normalization=`RAW`、calendar=`XNYS`；
- requested/evaluated range与 session inventory；
- signal→selection→intent→submit→fill chronology policy id；
- canonical DQ receipt/report scope、as-of、checksum与 status；
- input ordering-independent identity、source checksum、fee/slippage/fill identity；
- blocked reason codes、cash-preservation/no-order/no-fill disposition；
- external action、raw export、investment interpretation、paper/live/broker/production 全为 false/none。

Loader 必须拒绝 noncanonical bytes、symlink/path escape、extra/missing fields、hash/identity/range/scope/as-of mismatch、
forged PASS、DQ semantic FAIL/UNKNOWN、未 reviewed sensitivity role 与任何越权 activation。

## 5. Heuristic / policy boundary

DTE、moneyness、delta、spread、OI、volume、quote freshness、fee、slippage、latency、partial fill、cancel、expiry、
position sizing、initial cash 与 acceptance threshold 都会影响投资解释。本任务不得自行硬编码；在 Owner-reviewed
policy 缺失时，descriptor 必须输出 typed `OWNER_REVIEW_REQUIRED` / `POLICY_BLOCKED_CASH_PRESERVATION`，
且 order/fill 数量为零。纯协议常量、schema version 与 0/1 safety bounds 不属于投资 heuristic。

## 6. 开发阶段与验收

1. S0：2500 terminal review seal + 本任务登记；
2. S1：governed START/LANE、继承/hash/threshold audit；
3. S2：task-owned DAILY contract/policy/typed API 与 unit/property/golden tests；
4. S3：canonical DQ admission、window/chronology、cash-preservation replay；
5. S4：system flow、architecture fragments、generated task shadow、append-only compatibility authority；
6. S5：focused/adjacent/compatibility 与 final-tree Architecture→Contract→Integration→Reproducibility→exclusive Full；
7. S6：ordinary non-force push、SHA verify 与 cleanup。

完成本任务仍不会自动授权真实选券、订单模拟、完整历史 Cloud backtest或投资结论。后续每一项必须由独立任务和
Owner-reviewed policy/authorization 解锁。

## 7. 当前进度

2026-08-08：由 TRADING-2500 terminal reviewed GO 登记。实现尚未开始；当前只冻结继承、日级 chronology、
primary window、DQ/PIT/evidence admission、heuristic governance 和外部作用边界。
