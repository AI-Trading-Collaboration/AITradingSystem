# TRADING-2499：QQQ Options Daily Primary Backtest Contract V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1`

优先级：`P0`

状态：`BASELINE_DONE`

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

2026-08-08：由 TRADING-2500 terminal reviewed GO 登记。S1 governed START/LANE 已在 exact base
`2064a2e1855229f7260c725f8287174dc09b63f3` PASS，branch=
`codex/trading-2499-qqq-options-daily-primary-backtest-contract`；2501 docs-only candidate 与本任务路径
disjoint 并保持冻结。

S2/S3 已实现 task-owned policy、strict loader、`QQQOptionsDailyPrimaryBacktestRequest`、
`CanonicalDailyDQAdmission` 与 sealed `QQQOptionsDailyPrimaryBacktestDescriptor`。loader 会重放 2500 tracked
review 与所有 predecessor exact bytes；DQ admission 直接解析 canonical `DQReportRecord`，校验 scope、version、
DQ/PIT status、15 checks、as-of、range、repository code、source/checksum，拒绝 forged PASS、semantic
FAIL/UNKNOWN、hash/scope/as-of mismatch。session inventory 由 reviewed XNYS calendar 重算并排序，因此输入排列
不改变 identity；PRIMARY requested/evaluated start 固定为 `2021-02-22`，未 reviewed 的 sensitivity/proxy/stress
与 `2022-12-01` 默认路径 fail closed。

首轮 focused `25 passed / 2 failed`，两项均为测试夹具错误（legacy fixture 沿用错误结束日、dump 后 dict 被当作
model），生产合同未放宽；只修测试构造后以同一 `python -m pytest -n 16 --dist loadfile` 覆盖重跑，结果
`27 passed in 4.76s`。policy SHA-256=`4a060600ef9d532e75449a09628a54b84c9b68eca41989e1e4ed18de54b3109a`。
S4 DevEx generate/validate PASS：`1096 modules / 1260 tests / 856 direct writers / 0 violations`。
task-shadow 初次 generate 在隔离 worktree 因冻结 handoff 引用的四个 ignored runtime artifact 缺失而 fail closed；
从 clean main checkout 逐项核对 frozen SHA-256 后 hydrate exact bytes（fast-unit=`5afc81ae...53ca`、
Architecture=`a7c070c9...2c94`、Contract=`6994b8ed...68a9`、Full=`1785c2c6...a6a1`），原命令重跑
PASS：`967/462/505`、legacy/v2 byte-identical。严格 descriptor loader 增加 DQ report file replay 与内部
authority/source/range/as-of cross-binding 后，同覆盖 focused 为 `29 passed in 4.75s`，Ruff/mypy/compileall PASS。

compatibility/deprecation 首轮以完整 `python -m pytest -n 16 --dist loadfile` 覆盖 207 项，terminal 为
`124 passed / 83 failed in 240.92s`；根因为 2499 successor current-authority 集合漏接
`tests/test_trading2452_architecture_contract.py`，且 2500 terminal review 仍直接读取历史 raw hash，造成共享
authority 断言级联，并非 83 个独立语义缺陷。最小修复仅补 2499 additional supersession path、让 2500
历史记录通过最新 `_source_sha256` authority 解引用，并刷新 DevEx/current hashes；未修改历史 payload、prefix、
exact-byte/hash 验证或扩大 domain source delta。一次重跑因 shell wrapper 等待上限误设而丢失 terminal 输出，
root pytest 随后自然退出；该轮无可验证 node summary，明确不作为测试证据。完整捕获的第二轮为
`206 passed / 1 failed in 249.18s`，唯一失败是 2499 `sources` 中两条 registry 路径未按 casefold 排序；仅交换
这两条 source records 后，第三轮保持完全相同覆盖 terminal `207 passed in 246.88s`。首轮与第二轮作为
failure-fix parent 保留，第三轮为 focused compatibility/deprecation PASS；Architecture 仍未启动。

实现 baseline 已冻结为 `BASELINE_DONE`；当前继续 append-only compatibility/frozen inventory 与 final pre-audit，
正式 gates 尚未启动。后继真实 backtest engine 仍被
`OWNER_REVIEWED_SELECTION_EXECUTION_ACCOUNTING_LIFECYCLE_THRESHOLDS_NOT_GRANTED` 阻塞。
