# TRADING-2498：QQQ Options Daily Free Cloud Capability Gate V1

最后更新：2026-08-08

稳定任务 ID：`TRADING-2498_QC_QQQ_OPTIONS_DAILY_CAPABILITY_GATE_V1`

优先级：`P0`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

exact base：`d26041175eee499e5ee69c27202bef3ba364fb67`

production effect：`none`

broker action：`none`

## 1. Owner direction 与目标

Owner 于 2026-08-08 明确选择：先验证 QuantConnect Free Cloud 对日级 QQQ Options 主研究路径的
实际能力，只有 capability gate 为 OK 后才启动回测工程调整。

本任务只回答以下前置问题：

1. 当前 Free organization 是否能在项目 primary start 附近实际请求 QQQ Options DAILY 数据；
2. `2021-02-22..2021-02-26` 五个预期交易 session 是否可见 option chain；
3. 日级策略所需的 contract、bid/ask、open interest、Greeks/IV 是否至少存在可审计 aggregate；
4. actual requested/evaluated range、engine/build、data-point count、runtime 与 result artifact 是否可记录；
5. 在不下单、不下载 raw rows、不产生投资结论的情况下，Free Cloud 是否足以支持后继日级工程。

本任务不是策略回测、收益验证、完整 primary-window retention 证明或 license 法律意见。即使 gate
为 GO，也只允许登记并启动后继离线日级工程任务，不授权完整窗口 cloud run。

## 2. Inherited authority

TRADING-2498 必须继承且不得重定义：

- 2481 shared records、canonical seal/replay、policy envelope 与 export classification；
- 2482 DQ/PIT、chronology、calendar、cache/evidence identity；
- 2484 QuantConnect adapter safety 与 project/result mapping；
- 2489 strict manual evidence bundle 和 2490 reconciliation boundary；
- 2492 terminal disposition=`PILOT_NO_GO_LICENSE_OR_EVIDENCE`、processed data points=`734127`、
  reviewed cap=`250000` 与 single-use authorization 已失效；
- 2493 terminal signoff=`SIGNED_NO_GO`、aggregate=`NO_GO_KEEP_BLOCKED`；
- 2497 proposal aggregate=`NO_GO_KEEP_BLOCKED_PRIMARY_WINDOW_AND_SHARED_GATES`，proposal
  file/content SHA-256=`66d7a7b8fcf38fe56f210fc3ca927b14325548383d0c2c02ab0c37fca5348098` /
  `6b2c67dad95643ff7c43a502d921d5830eec037f2fe19c6fa5f64aaee99163ef`。

2498 不得把 2497 Owner acceptance、Free listing、provider coverage start、2025-12-02 单日 evidence
或 caller 自报 access 解释为新的 cloud authorization。

## 3. Frozen capability-run scope

外部运行只可在收到新的 exact Owner token 后执行；proposal 默认全部 external flags=false。

冻结 scope：

- target：已有 dedicated capability project；exact project id 在 proposal 中绑定；
- requested range：`2021-02-22..2021-02-26`；
- expected reviewed sessions：`2021-02-22`、`2021-02-23`、`2021-02-24`、`2021-02-25`、
  `2021-02-26`；
- underlying：`QQQ` Equity，`Resolution.DAILY`，`DataNormalizationMode.RAW`；
- derivative：QQQ Equity Options，`Resolution.DAILY`；
- intent：capability observation only；
- maximum project mutations：`1`；
- maximum cloud backtests：`1`；
- maximum orders：`0`；
- maximum fills：`0`；
- portfolio invested：必须始终 false；
- raw option rows logged/exported：必须 false；
- external evidence：只允许 export-safe aggregate logs、code/project identity、engine/build、
  backtest id、result JSON 与 UI screenshots/hash-only review records。

本范围的日期和次数是协议安全边界，不是投资阈值。任何扩大 requested range、增加 backtest、下单、
创建第二项目、下载数据或使用 API/CLI 都需要新的任务和 Owner authorization。

## 4. Required aggregate observations

每个预期 session 只记录聚合计数，不记录 symbol-level/raw option rows：

- `option_chain_present`；
- `contract_count`；
- `two_sided_quote_count`；
- `positive_open_interest_count`；
- `finite_greeks_count`；
- `finite_implied_volatility_count`；
- `raw_rows_logged=false`；
- `orders_submitted=0`。

run terminal 还必须记录：

- requested/evaluated start/end；
- Free organization 与 compute resource 人工观察；
- repository/proposal/policy/code identity；
- QuantConnect project id、backtest id、algorithm id、engine version/build id；
- processed data points、elapsed runtime；
- orders/fills/fees/portfolio-invested；
- result artifact byte count/SHA-256；
- independent reviewer、review timestamp、exceptions 与 missing evidence。

不得在日志中写 contract symbol、strike、expiry、bid、ask、OI、Greeks、IV 的逐行值，也不得通过
result artifacts 重建 option chain。

## 5. Gate taxonomy

terminal gate 只允许：

- `GO_FOR_DAILY_ENGINEERING_ONLY`：五个 expected sessions 全部 evaluated；每个 session 均有
  option chain、至少一个 contract、至少一个 two-sided quote、至少一个 positive OI、至少一个
  finite Greeks 与 IV observation；identity/evidence 完整；orders/fills=0；无 scope violation；
- `NO_GO_CAPABILITY_OR_ENTITLEMENT`：明确无 entitlement、缺失 required daily fields、run/build
  失败、任一 expected session 无 chain，或发生任何 prohibited action；
- `UNKNOWN_EVIDENCE_INCOMPLETE`：artifact、identity、review、range、session 或 checksum 证据不全，
  不得当作 PASS。

`GO_FOR_DAILY_ENGINEERING_ONLY` 只解除“是否值得开发日级 adapter/selector/execution contract”的
前置未知，不解除：

- 完整 `2021-02-22..present` historical retention；
- 2485 selection、2486/2487/2488 policy blocked；
- 2489 evidence collection 与 2490 reconciliation；
- option-event DQ/PIT；
- raw download、paid tier、API/CLI、range expansion；
- investment interpretation、paper/live/production/broker。

## 6. Owner authorization boundary

工程线必须先生成 deterministic proposal，提供 proposal file/content、policy file/canonical、authority
set 与 code SHA-256。Owner token 必须 exact-bind 这些 hashes，并至少冻结：

- requested range 和 expected sessions；
- target project id；
- maximum project mutation/backtest/order/fill；
- allowed/prohibited actions；
- token expiry、single-use 与 evidence-collection invalidation；
- collector 与 independent reviewer。

在 exact token 收到前：

- 不登录 QuantConnect；
- 不创建或修改 project；
- 不运行 cloud backtest；
- 不调用 API/CLI/HTTP/Object Store；
- 不下载数据、不购买、不订阅；
- 不执行 paper/live/broker/production。

2498 不复用 2480/2492 token；普通自然语言同意不能替代 exact hash-bound token。

## 7. Acceptance criteria

Offline proposal baseline：

- task-owned strict policy、sealed proposal/observation/gate records；
- 2493/2497 exact canonical replay；
- date/range/session、resolution、zero-order、single-use、allowed/prohibited boundary deterministic；
- missing/reordered/duplicate session、wrong project/range/resolution、orders/fills、raw rows、scope
  expansion、forged GO、unknown→PASS、tamper/noncanonical/permutation negatives fail closed；
- system flow、architecture fragments、task register、generated/task shadow/current authority 同步；
- focused/adjacent/compatibility 与 final-tree formal gates PASS；
- external action remains none until Owner token。

External capability gate：

- exact token canonical replay；
- one and only one bounded run；
- complete export-safe evidence and independent review；
- terminal gate typed and hash-bound；
- only `GO_FOR_DAILY_ENGINEERING_ONLY` permits registering the successor engineering task。

## 8. Sequencing

1. S0：task row、requirement、exact scope 与 successor stop condition；
2. S1：offline policy、typed proposal/observation/gate public API；
3. S2：negative/property/golden tests、system-flow/generated authority；
4. S3：formal validation、ordinary main push、cleanup；
5. S4：Owner reviews exact hashes and issues a single-use token；
6. S5：manual UI execution and export-safe evidence collection；
7. S6：independent review and terminal gate；
8. S7：仅在 GO 后登记后继
   `TRADING-2499_QQQ_OPTIONS_DAILY_PRIMARY_BACKTEST_CONTRACT_V1`。

## 9. Current progress

2026-08-08：Owner 指示先安排能力验证，确认能力 OK 后才推进工程。READ_ONLY preflight 在 exact
main `d26041175eee499e5ee69c27202bef3ba364fb67` PASS；TRADING-2498 此前未登记。本次仅执行
coordinator-owned S0 task row/requirement mutation，Owner token 尚未授予，QuantConnect/cloud/项目/
API/CLI/HTTP/Object Store/raw download/paper/live/broker/production 动作均为 none。

SINGLE_LANE START/LANE 随后从同一 exact base PASS，`contract_change=false`。task-owned policy、
strict loader、2497 proposal canonical replay、sealed proposal/session observation/run observation/gate
record、typed three-way decision、negative/property/golden tests、architecture fragments 与 system flow
已实现。focused 首轮 pytest=`51 passed`，strict mypy PASS；Ruff 首轮仅有 4 条 line-length 与 1 条
unused-import 静态失败，没有 pytest node FAIL。最小格式修复后同覆盖=`51 passed`；tracked proposal
loader 加入后同覆盖=`52 passed`，Ruff/format/strict mypy 全部 PASS。

implementation authority=`676d6b1429ee1ef60fbfc4de1d62f9d6ee9184ce`。由该 exact commit 生成的
canonical proposal：

- repository path=`inputs/external_validation/qc_qqq_options_daily_capability_gate_proposal_20260808.json`；
- file/canonical SHA-256=`6b226751453bc2d73e0e5ec14be6975124e3a0948435ff7282658a3c2fe3e5dc`；
- content SHA-256=`98566866892b081ad1011e7388348c780e506018e94d568f83b1fcef888a7f95`；
- policy file/canonical SHA-256=`0036996a1d4e9928f2f4b537a3e4158ada2efd15dc24cf6b0918467a1f647812` /
  `1ec345fdf36a101023eacaff6ca78450bd54b45290758438f0ae4a56b2ff63f9`；
- authority-set SHA-256=`b0f4145dfaf00bfbf90a905b80deab65cf10a0686221974a83a71a885a4a4908`；
- proposed action=`authorize_single_zero_order_qc_daily_capability_gate_v1`；
- authorization/gate=`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS` /
  `UNKNOWN_EVIDENCE_INCOMPLETE`。

proposal 仍只是一份 Owner-review request，不授权任何 external action。必须先完成 generated/current
authority 与 formal final-tree validation，再 ordinary push exact hashes；Owner 此前的自然语言方向不能
替代后续 single-use exact token。

兼容层首次以完全覆盖的 `-n 16 --dist loadfile` 运行得到 `199 passed / 2 failed`；两项均为新增
module/test 导致的 ARCH-004G frozen inventory 与 task-shadow v2 current-authority 漂移，没有领域逻辑或
pytest node 语义失败。该运行只作为 failure-fix parent；最小修复仅冻结 1093/1257 inventory 并追加
TRADING-2498 current-authority section，不重写历史 prefix、不降低 source/hash 验证。任务工程基线因此转为
`BASELINE_DONE`，但 external capability 仍被 Owner token 阻断，后继 TRADING-2499 仍未登记、未启动。
