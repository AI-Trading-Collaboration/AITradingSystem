# DATA-GOV-001 D0B2B Canonical Daily Acceptance Remediation

最后更新：2026-07-25

状态：`IN_PROGRESS`

稳定任务 ID：
`DATA-GOV-001_D0B2B_CANONICAL_DAILY_ACCEPTANCE_REMEDIATION`

父任务：`DATA-GOV-001_UNIFIED_DATA_FOUNDATION_GOVERNANCE`

关联任务：

- `ARCH-004G4_OPERATIONS_PERIODIC_CONSUMER_MIGRATION`
- `OPS-067_READER_BRIEF_QUALITY_FAIL_CLOSED_FINALIZATION`

优先级：`P0`

owner：project owner / data platform owner / architecture coordinator

production effect：`none`

Owner decision：

- decision id：
  `owner_decision:DATA-GOV-001-D0B2B:2026-07-25:approve_vix_xnys_aligned_v1`；
- decided at：`2026-07-25`；
- decision：采用方案 A — `XNYS decision-session aligned`；
- rationale：保持 canonical daily score、跨 ticker feature 和研究窗口使用单一 XNYS
  decision calendar，同时完整保留 Cboe raw/immutable bytes、source provenance 和受治理
  normalization audit；不在本阶段引入 per-asset calendar 的 feature/backtest/report
  全链迁移风险；
- intended effect：canonical `prices_daily.csv` 只发布 XNYS decision-session rows，
  `^VIX` 非 XNYS session rows 仅在 reviewed normalization 中排除并记录，不删除原始证据；
- review condition：若未来策略确需利用 Cboe-specific extra sessions，必须另行登记
  per-asset calendar migration，并证明跨 calendar alignment 与 no-look-ahead。

## 触发事件

2026-07-25 09:30 Asia/Tokyo 的唯一 canonical scheduler trigger 已通过
`aits ops daily-run` 运行 `as_of=2026-07-24`：

- run id：
  `daily_ops_run:2026-07-24:20260725T003257Z`；
- run-control key：
  `operations_run_3f036b9fc7d836160b483be8`；
- bundle：
  `outputs/runs/daily/20260725T003257Z/as_of_2026-07-24__daily_ops_run_2026-07-24_20260725T003257Z/`；
- result：`FAIL`；
- ledger：`download_data=PASS`、`validate_data=FAIL`、其余 34 steps=`BLOCKED`；
- state SHA-256：
  `678ae3cc18debe4406583af2ae2fd1537da800867322106d2f64788008b33f9a`；
- ledger SHA-256：
  `a5a31dca2e6a69d124eab105bb43a005f77b6a7c9eee3aee75fdacb645d9d3a2`。

同一 key 的 `validate_data` attempt budget 已耗尽。旧 2026-07-22、2026-07-23
FAILED state/ledger 和本次 FAILED state/ledger 都是保留证据，不得删除、编辑、改 spec
或通过新 key/手工子命令绕过。

## 已通过的能力

本次真实运行证明 Wave14/Wave15 的下列修复已经生效，不能把本任务误写成这些能力的回退：

1. `download_data` 发布了 staged immutable composite generation。
2. 最终 `prices_daily.csv` 的完整文件记录绑定 54,662 rows、SHA-256
   `d50d57be04e655fe3be4efd5e831f86eb3afb7623dc34656de19cdb3c9b40df7`。
3. winning-row provenance 精确闭合为：
   canonical predecessor 54,636 + FMP 25 + Cboe 1 = 54,662。
4. Marketstack、rates 与 download manifest 的完整文件 bytes、row count、SHA 也与 publication
   member 一致。
5. `validate_data` 的真实命令包含
   `--execution-profile daily_default.v1`，并发布了对应 profile/as-of discovery pointer。
6. receipt
   `dq_execution_a8a1e1653c0508a1e89ed27f708d1b4e7ea9954579247969ca3def1ad50e41a0`
   已绑定 reviewed policy、exact invocation、input/manifest/report bytes 与真实 step 时间区间。
7. strict DQ FAIL 后 PIT、score、dashboard/latest、Reader Brief、quality、health、secret scan
   均保持零执行；无 production/active-shadow weights、broker、order 或 trading effect。

因此本任务只重开 D0B2 的 operational semantic acceptance，不否定已完成的 publication
transaction、same-byte capture、profile propagation 或 D0B3 consumer-scoped authorization
工程基线。

## 真实 blocker

### B1：publication window 与 DQ window 被错误要求完全相等

canonical publication 的 requested window 为
`2018-01-01..2026-07-24`，daily DQ requested window 为
`2021-02-22..2026-07-24`。前者完整覆盖后者，且 Wave15 consumer authorization 已采用
`publication_start <= receipt.requested_start`、末端 exact match 的语义；但
`data/quality.py` 仍对两个 tuple 做完全相等比较，产生
`download_manifest_requested_window_mismatch`。

这是实现/集成缺陷，不是需要放宽 DQ 的策略选择。修复必须保持 fail closed：

- publication start 不得晚于 DQ start；
- publication end 必须与 requested end 精确相等；
- publication/source/manifest/current generation 仍须绑定 exact bytes；
- 覆盖不足、末端漂移或跨 generation 继续阻断。

### B2：US equity calendar 缺少特殊全日休市

`prices_internal_trading_day_gap` 的 25 个缺口全部是 25 个股票/ETF 在
`2025-01-09` 缺行。该日为 President Jimmy Carter National Day of Mourning，
NYSE 官方 memo 明确记录 market closure：

`https://www.nyse.com/publicdocs/nyse/markets/american-options/rule-interpretations/2025/National_Day_of_Mourning_20250102.pdf`

当前 `trading_calendar.py` 明确声明只覆盖 regular full-day holiday rules，
不包含 unscheduled special closures，因此把真实休市日误判成逐 ticker gap。

修复不得补造价格或给 gap 加静默例外。必须建立 versioned special-closure policy/registry，
至少记录 date、market/calendar id、closure type、reason、authoritative source、owner、
policy version、review condition，并让 resolver、DQ、receipt verifier 使用同一 contract。

### B3：`^VIX` 与股票/ETF 共用单一 XNYS session 语义

`prices_non_market_session_date` 的 30 个日期全部且仅来自 `^VIX`。这些行覆盖
Memorial Day、MLK Day、Thanksgiving 等 XNYS regular holiday，具有真实 OHLC 值。
当前 publication 对历史 predecessor 只承诺 immediate-source lineage，不能据此补写完整
origin provenance；但 Cboe 官方 VIX history 页面和本项目使用的官方 endpoint 均证明
`^VIX` 是独立 index data family：

- `https://www.cboe.com/tradable_products/vix/vix_historical_data`
- `https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv`

不能把统一 XNYS calendar 强加给所有 asset，也不能简单把 `^VIX` 加入免检名单。

Owner 已审核以下语义，并于 2026-07-25 选择方案 A：

1. **A — XNYS decision-session aligned（推荐窄版）**  
   canonical `prices_daily.csv` 只保留 XNYS decision sessions；Cboe raw/GTH bytes 和
   source provenance 保留在 raw/immutable publication，normalization 显式、可审计地排除
   非 XNYS session 的 `^VIX` 行。这样 daily score、研究窗口与跨 ticker 对齐仍使用同一
   decision calendar。
2. **B — per-asset calendar（本轮未选择）**
   为每个 asset/source 引入 `calendar_id`，允许 `^VIX` 使用 Cboe-specific sessions；
   所有 feature、alignment、coverage、backtest 与 report consumer 必须明确证明跨 calendar
   对齐和 no-look-ahead。该方案长期更一般，但范围、迁移与验证成本更高。

在 owner 决策前不得通过删行脚本、warning 降级、ticker 白名单或跳过 calendar check
让 canonical run 变绿。

### B4：strict consumer 仍会被 unresolved warning 阻断

本次 DQ 还有一个 `prices_adjustment_ratio_jump` warning，共 5 rows：

- AMZN `2022-06-06`
- GOOG `2022-07-18`
- NVDA `2021-07-20`
- TQQQ `2022-01-13`
- TQQQ `2025-11-20`

Wave15 `daily_score_daily@1.0.0` 只接受 strict `PASS`，
`PASS_WITH_WARNINGS` 仍必须在 runner 前阻断。因此即使 B1～B3 消除全部 error，
也不能宣称 canonical acceptance 完成。

这些记录必须逐项以发行人、基金 sponsor、交易所或其他 reviewed authoritative source
核验 split/adjustment 事件和比例，再写入 reviewed policy；若不能证明为已知事件，应继续作为
数据调查 blocker。不得放宽 strict consumer profile。

### B5：receipt 未完整绑定 calendar decision authority

当前 receipt 的 `implementation_sources` 包含
`immutable_publish.py`、`quality.py`、`quality_execution.py`，但未绑定实际决定 session
判定的 `trading_calendar.py` 或 versioned calendar policy bytes/SHA。修复后 receipt、
invocation、publisher/verifier 与 D0B3 authorization 必须能检测 calendar policy 或实现漂移，
不能只绑定 DQ wrapper。

## 实施分片

### S0：owner policy carrier

- 记录 A/B 选择、owner、version/status、rationale、intended effect、review condition；
- 冻结 special-closure registry schema 和 authoritative-source requirement；
- 冻结 adjustment-event review scope；
- 明确不会修改 research default start `2021-02-22`、score/position thresholds、
  production 或 broker 行为。

### S1：window containment

- 把 publication/DQ window 校验改为 reviewed containment contract；
- 增加 start coverage不足、end drift、跨 generation、daily-default真实组合正负回归；
- receipt requested window 继续精确记录 `2021-02-22..as_of`。

### S2：calendar authority 与 receipt binding

- 新增 versioned special-closure registry，至少覆盖 `2025-01-09`；
- 统一 resolver、DQ expected sessions、coverage/internal-gap 的 calendar authority；
- receipt/verifier 绑定 calendar policy path/SHA/version 和必要 implementation sources；
- policy/source/tamper/drift 必须 fail closed。

### S3：`^VIX` session contract

- 按 owner 选择实现 XNYS-aligned normalization 或 per-asset calendar；
- raw/provider bytes、publication lineage、过滤/对齐计数和原因可审计；
- 不允许 decision-time look-ahead、静默 drop 或 source provenance 丢失。

### S4：adjustment warning closure

- 用权威来源逐项核验 5 个事件；
- reviewed policy 升版并记录 event date、ratio、source、owner/review condition；
- 未核验项保持 warning/blocker，不使用 blanket threshold。

### S5：验证与 canonical acceptance

- focused parallel tests：
  `test_data_quality.py`、`test_trading_calendar.py`、
  `test_data_quality_execution*.py`、`test_data_quality_consumer_authorization.py`
  及真实 daily plan/CLI propagation regression；
- architecture、contract、integration、reproducibility 和风险相称的 Full validation；
- 刷新 task registry shadow、compatibility/source hashes 和受影响 flow/runbook；
- 只在新合法 provider-ready trading date、无 writer/lease、scoped tree clean 时，
  通过唯一入口 `aits ops daily-run` 做运营验收；
- 不重试或修改 `operations_run_3f036b9fc7d836160b483be8`。

## 退出标准

1. reviewed `daily_default.v1` DQ 为 strict `PASS`，error=0、unresolved warning=0。
2. publication 对 DQ requested window 提供可证明的完整覆盖，末端 exact match。
3. `2025-01-09` 被同一 versioned calendar authority 识别为特殊全日休市。
4. `^VIX` extra-session rows 按 owner-approved contract 处理，raw bytes/provenance 保留，
   下游 alignment 无 look-ahead。
5. calendar policy/implementation exact bytes 或 version 被 receipt/verifier/consumer
   authorization 绑定，任一漂移 fail closed。
6. 5 个 adjustment events 全部获得权威来源治理，或未通过项继续阻断而不伪造 PASS。
7. 新合法 canonical run 完成
   download -> strict DQ -> PIT -> score -> dashboard/latest -> Reader Brief ->
   validate-reader-brief，全链 artifacts/manifest/finalization 可验证。
8. 旧 2026-07-22、2026-07-23、2026-07-24 FAILED state/ledger 原字节保持。
9. periodic due/not-due/blocked 只由同次 canonical evidence 决定，不单独拼装。
10. `production_effect=none`；无 production/active-shadow weight、broker/order/trading。

## 当前实施状态

Owner 已通过
`owner_decision:DATA-GOV-001-D0B2B:2026-07-25:approve_vix_xnys_aligned_v1`
选择方案 A，任务保持 `IN_PROGRESS`，实施阶段为
`ENGINEERING_VALIDATION_PASS_CANONICAL_ACCEPTANCE_PENDING`：

- S0 PASS：owner decision、两个reviewed policy carrier和五条权威复核split事件已冻结；
- S1 PASS：publication containment为起点覆盖、末端exact，覆盖不足/末端漂移/前代
  generation继续阻断；
- S2 PASS：reviewed XNYS special-closure registry覆盖`2025-01-09`，所有session resolver共用
  该权威；canonical DQ validator v2把calendar policy commitments与实现源码加入receipt binding；
- S3 PASS：`^VIX`在canonical publication前按XNYS decision sessions对齐；Cboe raw bytes、
  cache provenance、policy SHA及逐event排除日期/数量/原因进入受transaction绑定的audit；
- S4 PASS：AMZN、GOOG、NVDA和TQQQ五条事件以发行人/基金sponsor官方资料治理；
  未登记ratio jump仍为WARNING；
- S5 engineering PASS：focused slice分别`49`、`32`、`103`、receipt `38`项通过，
  combined parallel regression `331 passed`，Ruff PASS；
- S5 formal engineering PASS：architecture-fitness=`617 passed`、
  contract-validation=`275 passed`、integration=`993 passed`、
  reproducibility=`23 passed`，自然Full=`7,210 passed / 3 skipped / 643 warnings`
  （`1,163.57s`）；对应runtime artifacts分别为
  `architecture-fitness_20260725T033436Z`、`contract-validation_20260725T033617Z`、
  `integration_20260725T033839Z`、`reproducibility_20260725T033936Z`和
  `full_20260725T034024Z`。

剩余唯一业务验收是退出标准第1和第7项：在新合法provider-ready trading date通过唯一入口
`aits ops daily-run`形成新的strict canonical evidence。不得自动安排`2026-07-24`同key重试，
不得直接运行PIT/score等子命令，也不因工程验证通过而把旧strict canonical receipt写成可用。
