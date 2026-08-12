# TRADING-2512：QC QQQ Options Primary Window Export-Safe Derived Aggregate Collector Contract V1

最后更新：2026-08-12

稳定任务 ID：`TRADING-2512_QC_QQQ_OPTIONS_PRIMARY_WINDOW_EXPORT_SAFE_DERIVED_AGGREGATE_COLLECTOR_CONTRACT_V1`

优先级：`P0`

状态：`IN_PROGRESS`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

## 1. 目标

在 TRADING-2511 derived calibration evidence generator 之后，建立一个严格离线、可审计、可重放的
QuantConnect QQQ Options DAILY primary-window derived-aggregate collector contract 与 deterministic project-code
package。它定义未来一次另行授权的零订单 Cloud backtest 如何把 option-chain 事实压缩成 export-safe
per-session aggregates，再由本地 strict parser 还原为 2511 source observations。

本任务只实现合同、policy、project-code renderer、Download Results JSON parser 与离线 DQ/PIT handoff；不登录
QuantConnect，不创建或修改项目，不运行 Cloud backtest，不调用 API/CLI/HTTP/Object Store，不下载或导出 raw
option rows，不购买数据，不选择或建议投资阈值，不激活 selection、engine 或 backtest。

## 2. 冻结基线与继承

- registration base / exact latest main：`7fb7d17770c520a601450081500bda1906648992`；
- primary research start：`2021-02-22`；role：`PRIMARY`；exchange calendar：`XNYS`；
- 2511 policy file SHA-256：
  `ec607e9d34d3ee454b997575fa94357cb3cc349bca0216c067a0a30b332d6a82`；
- 2511 implementation file SHA-256：
  `2957d40aac5b2f595b4a6d4d8aed3cfd86c2b2c6517ae52581bb3eecb681c7b1`；
- 继承 2481 shared records/envelope、2482 DQ/PIT identity、2484 QC adapter、2499 DAILY chronology、
  2500 reviewed daily capability、2509 v2 slot catalog、2510 admission/readiness 与 2511 generator；不得复制、
  重定义或弱化这些 authority；
- 2502/2504/2507 仍无 Owner-supplied policy values，engine 固定
  `POLICY_BLOCKED_CASH_PRESERVATION`。

## 3. 官方平台约束与结果载体决策

2026-08-12 对 QuantConnect 官方文档的只读核验确认：

1. Free tier 每次 backtest log 配额为 `10KB`，且官方明确声明 logs 不得用于导出 dataset information：
   `https://www.quantconnect.com/docs/v2/cloud-platform/organizations/resources`；
2. Free tier custom chart 配额为最多 `10` series、每个 series 最多 `4,000` data points，超额会跳过数据或
   停止新增 series：`https://www.quantconnect.com/docs/v2/writing-algorithms/charting`；
3. Backtest Results 的 `Download Results` JSON 包含 runtime statistics、charts、overview 与 orders：
   `https://www.quantconnect.com/docs/v2/cloud-platform/backtesting/results`；
4. LEAN `Series.AddPoint(DateTime, decimal)` 支持显式时间点：
   `https://www.lean.io/docs/v2/lean-engine/class-reference/cs/classQuantConnect_1_1Series.html`。

因此 collector：

- 禁止把 per-session aggregates 写入 logs；logs 不是 evidence carrier；
- 禁止 Object Store；
- 仅使用一张固定 chart 的 exact `10` 个 series 与少量 manifest-level runtime statistics；
- 证据只从 Owner 手动下载的完整 Results JSON 导入，不使用 API/CLI/HTTP；
- 任一缺失、截断、重复、乱序、未知 series、超 quota、timestamp/ordinal drift 均 fail closed；
- chart 是受限 transport，不是策略图表或投资结论。

## 4. 可观测 slot 边界

### 4.1 本 collector 可生成的 exact 9 slots

1. `SEL_DELTA_SOURCE_RANGE`：`delta_max` / `delta_min`；
2. `SEL_DTE_WINDOW`：`dte_days_max` / `dte_days_min`；
3. `SEL_MONEYNESS_RANGE`：`moneyness_ratio_max` / `moneyness_ratio_min`；
4. `SEL_OPEN_INTEREST_FLOOR`：`open_interest_max` / `open_interest_min_nonzero`；
5. `SEL_RANK_PRIORITY`：`candidate_count` / `deterministic_tie_count`；
6. `SEL_SPREAD_LIMIT`：`relative_spread_max` / `relative_spread_min`；
7. `SEL_VOLUME_FLOOR`：`volume_max` / `volume_min_nonzero`；
8. `EXE_MARKETABLE_LIMIT`：`ask_price_max` / `ask_price_min`；
9. `EXE_QUOTE_DISPOSITION`：`missing_quote_count` / `one_sided_quote_count` /
   `two_sided_quote_count`。

这些值只描述 observed envelope/count，全部 `is_policy_value=false`，不得解释为阈值、推荐区间、选券条件、
liquidity gate、execution baseline 或投资结论。

### 4.2 明确不由 DAILY 零订单 collector 生成的 slots

- `SEL_QUOTE_FRESHNESS`：DAILY option-chain timestamp 不能证明 intraday quote age；不得伪造 `0 seconds`；
- `ACC_CASH_RESERVATION`、`ACC_FEE_SCHEDULE`、`ACC_SIZING_EXPOSURE`：零订单采集没有可解释的 accounting
  outcome；
- `ACC_DQ_PIT_REPRO`、`ACC_RESULT_INCLUSION`、`ACC_SAMPLE_COVERAGE`：由本地 admission/validation contract
  生成，不能把平台 chart 当作 validation PASS；
- `LIFE_EXPIRY_EXIT_GUARD`、`LIFE_TERMINAL_VALUATION`：没有持仓生命周期，不能伪造 terminal facts。

unsupported slot 必须保持 `NOT_COLLECTED_BY_DAILY_ZERO_ORDER_CONTRACT`，不得用零、空数组或 fixture 补齐。

## 5. Ten-series transport map

固定 chart id：`TRADING2512_EXPORT_SAFE_DERIVED_AGGREGATES_V1`。

1. `S01_DELTA_RANGE`：每 session 两点，ordinal 1=`delta_max`、2=`delta_min`；
2. `S02_DTE_WINDOW`：ordinal 1=`dte_days_max`、2=`dte_days_min`；
3. `S03_MONEYNESS_RANGE`：ordinal 1=`moneyness_ratio_max`、2=`moneyness_ratio_min`；
4. `S04_OPEN_INTEREST`：ordinal 1=`open_interest_max`、2=`open_interest_min_nonzero`；
5. `S05_RANK_PRIORITY`：ordinal 1=`candidate_count`、2=`deterministic_tie_count`；
6. `S06_SPREAD_RANGE`：ordinal 1=`relative_spread_max`、2=`relative_spread_min`；
7. `S07_VOLUME_RANGE`：ordinal 1=`volume_max`、2=`volume_min_nonzero`；
8. `S08_ASK_RANGE`：ordinal 1=`ask_price_max`、2=`ask_price_min`；
9. `S09_QUOTE_DISPOSITION_A`：ordinal 1=`missing_quote_count`、2=`one_sided_quote_count`；
10. `S10_QUOTE_DISPOSITION_B`：ordinal 1=`two_sided_quote_count`。

project code 用 `Series.AddPoint` 把每个 statistic 写入 session date 的 reviewed XNYS close 后固定秒级 ordinal；
Results JSON 中 Unix timestamp 必须可无歧义还原为 XNYS session date + ordinal。每个两点 series 的上限为
`2,000` sessions，取自 `4,000 / 2` 的 transport quota，不是投资阈值。proposal 必须显式给出 evaluated end 与
exact session inventory，并在运行前证明 session count 不超过该上限；禁止静默截断、抽样、降采样或自动拆分。

## 6. Project-code 与运行提案合同

project-code renderer 必须：

- 要求调用者显式提供 reviewed `requested_end/evaluated_end` 与 exact XNYS session inventory；不提供默认 end；
- 固定 `QQQ Equity DAILY RAW` 与 `QQQ Option DAILY` subscription；
- 对每个 session 只保留 aggregate accumulators，不保留或输出 raw contract rows；
- 使用 finite checks、positive underlying、nonnegative counts、two-sided quote 与 deterministic contract identity；
- 不应用 DTE/moneyness/delta/spread/OI/volume 或 rank threshold；candidate universe 是当日所有通过纯数据有效性
  检查的 observed contracts；
- `orders=0`、`fills=0`、`portfolio_invested=false`；任何 order event 立即标记 run invalid；
- runtime statistics 只记录 schema/code/policy/range/session/chart-map identity 与 no-order terminal status；
- 不使用 `Debug/Log` 承载 derived observations，不使用 Object Store、network、secrets、API 或 CLI；
- code bytes、policy、2511 metric catalog、repository SHA、proposal、authorization 与 result JSON 全部 exact hash
  cross-binding。

本任务只产生 `NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS` proposal baseline。未来实际运行必须另立并 ordinary
push exact proposal，由 Project Owner 单次授权 project mutation/backtest/evidence collection 范围；不得复用
2480、2492、2498 或 2500 token。

## 7. Results JSON strict admission

本地 parser 只接受 canonical manual Download Results bytes，并重验：

- exact project id、backtest id、project-code hash、proposal/authorization/policy/repository identities；
- exact chart name、10-series inventory、series type/unit/index、每个 series exact point count；
- point timestamp 单调、XNYS session/ordinal 可逆、session inventory 完整且没有周末/假日/额外 session；
- finite decimal、per-statistic domain、max/min consistency、quote disposition count consistency；
- result orders inventory empty、runtime total orders/fills/fees/volume 为零、portfolio 未投资；
- no raw option rows、no logs-as-data、no second run、no range expansion、no prohibited action；
- JSON file checksum、content checksum 与 canonical evidence seal。

解析成功只生成 collector evidence，DQ 状态仍为 `NOT_EVALUATED_PENDING_LOCAL_DQ_GATE`。只有调用 2482 canonical
DQ/PIT code path 并满足 exact 15-check PASS、source/range/as-of/checksum/repository/policy/contract identity 后，才可
构建 2511 source bundle。调用者自报 PASS、arbitrary report bytes、FAIL/UNKNOWN/NOT_EVALUATED 或 scope/hash mismatch
全部停止 handoff。

## 8. 安全不变量

所有 policy、proposal、evidence 与 handoff 固定：

- `owner_policy_value_count=0`；
- `executable_policy_authorized=false`；
- `engine_status=POLICY_BLOCKED_CASH_PRESERVATION`；
- `selection_authorized=false`；
- `orders=0`、`fills=0`；
- `external_action_authorized=false`；
- `investment_interpretation_allowed=false`；
- `raw_options_data_export_allowed=false`；
- `paper_allowed=false`、`live_allowed=false`、`broker_allowed=false`；
- `production_effect=none`、`broker_action=none`。

## 9. 实现与验证计划

### S0：registration / authority audit

- canonical task row + supporting requirement；
- governed START/LANE preflight；
- 2511 exact hashes、18-slot inventory、official quota、no-threshold 与 no-external audit。

### S1：collector contract/package

- task-owned policy、strict loader、typed proposal/evidence/result transport models；
- canonical seal/from-json/replay；
- deterministic project-code renderer 与 exact code hash；
- strict Download Results parser 与 2511 observation adapter。

### S2：fail-closed coverage

- unit/property/golden：input/result permutation identity、missing/extra/duplicate series/point/session、quota overflow、
  timestamp/ordinal/DST/calendar drift、NaN/Inf/domain/max-min/count mismatch、range/repository/policy/code/proposal/hash drift；
- orders/fills/nonzero portfolio、log/object-store/network/raw-row markers 与 fake authorization fail closed；
- unsupported slots 永不生成；fixture 永不进入 production inventory。

### S3：共享 wiring 与收口

- system flow、architecture fragments、task registry/generated/compatibility authority；
- Atlas 披露“collector contract 已实现、actual authorized run/evidence 仍未提供”；不得把工程绿色解释成策略有效；
- focused/adjacent/compatibility 后，在 final tree 串行完成
  Architecture→Contract→Integration→Reproducibility→exclusive Full；
- ordinary non-force push、SHA verify、branch/worktree cleanup。

## 10. 验收标准

1. exact 10-series transport 可在 Free quota 内无损承载 reviewed primary-window exact session inventory。
2. 合法 Results JSON 不受 object/series/point 输入排列影响，生成 byte-identical collector evidence。
3. truncated/downsampled/tampered/extra/missing/noncanonical result、任何 order/fill/raw/prohibited marker 均 fail closed。
4. 只生成 exact 9 supported slots；其余 9 slots 保持 typed unsupported/not-evaluated，不填零。
5. 2511 handoff 必须经 canonical 2482 DQ/PIT exact 15-check gate，不能信任调用者声明。
6. 无新增投资阈值、无 Owner policy value、无外部 action、无 engine/backtest/investment/production/broker effect。
7. focused、compatibility、generated authority、Atlas disclosure 与 final five-tier gates PASS。

## 11. 当前 blocker / 后继

当前 blocker：`OWNER_AUTHORIZED_PRIMARY_WINDOW_DERIVED_AGGREGATE_RUN_NOT_PROVIDED`。

本合同 baseline 完成后，后继顺序为：

1. ordinary-pushed exact run proposal（显式 end/session inventory/project/code/result carrier）；
2. Project Owner 单次外部动作授权；
3. 零订单 QuantConnect DAILY Cloud collection run 与 manual Download Results evidence；
4. independent review + 本地 strict parser + 2482 DQ/PIT；
5. 2511 generator → 2510 admission → Project Owner per-slot policy review；
6. 只有获得 typed G2 values/rationale/evidence/review/expiry 后，才可进入 executable-policy serial contract wave。
