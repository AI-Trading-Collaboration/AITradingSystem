# TRADING-2534 — QQQ Options 分阶段 DQ/PIT 研究准入与 export-safe 证据权威 V1

- priority: `P0`
- status: `DONE`（终态候选；以 final-tree 五级校验与 ordinary push 为 RELEASE 条件）
- owner: Codex capability coordinator（最小串行 contract wave）；Project Owner（任何后续外部动作与策略阈值）
- owner decision: `owner_decision:TRADING-2534:2026-08-18:authorize_high_risk_blocker_remediation_serial_contract_wave_v1`
- governed mode: `SINGLE_LANE`
- contract change: `true`
- frozen base: `a887aee4e0d0cfe396a9f7e6994a46afa0c9fe44`
- production effect: `none`
- broker action: `none`

## 1. 为什么先处理这个阻塞

TRADING-2533 对 TRADING-2532 的唯一 zero-order external validation 完成了离线准入，结果为
`1 PASS / 1 FAIL / 13 NOT_EVALUATED`：1202 个 session 中仍有 1 个 final never-chain session，
`DQ=FAIL`、`PIT=NOT_EVALUATED`。这是真实阻塞，不能改写成 PASS。

同时，现有 2482 evaluator 把研究数据、shadow selection 与 order/fill execution 的 15 项检查聚合为
同一个最终状态。zero-order transport run 按设计不会产生 signal、selection、order 或 fill，因此即使数据
证据完整，也无法在进入研究前证明执行阶段时序。provider 不暴露 raw checksum 时，
`provider_raw_checksum` 也会永久保持 `NOT_EVALUATED`，即使平台内 derived evidence 已有完整身份和封印。

本任务修复的是上述结构性阻塞，不修饰现有失败：

1. 保留 2482 `DQReportRecord`、15 checks、reason codes 和 fail-closed 事实不变；
2. 新增独立、typed、sealed 的 staged readiness authority；
3. 把 `DATA_RESEARCH`、`SHADOW_SELECTION`、`EXECUTION` 三个门按实际发生顺序分离；
4. 定义 provider raw checksum 不可得时唯一允许的 platform-attested derived evidence 路径；
5. 当前 2533 的 `chain_presence=FAIL` 必须继续阻塞所有阶段；
6. 本轮不访问 QuantConnect，不运行 Cloud，不导出 raw rows，不产生订单或成交。

## 2. 不变量与禁止事项

- 2482 V1 policy、evaluator、required check ids、现有 `DQReportRecord` 与历史报告 immutable；
- downstream-stage `NOT_EVALUATED` 不能被改写为 PASS，只能在更早阶段的 applicability 之外保留；
- 任一适用于当前阶段的 `FAIL` 必须使当前阶段及后继阶段 `BLOCKED`；
- 任一适用于当前阶段的未知、缺失或 `NOT_EVALUATED` 必须使当前阶段 `NOT_READY`；
- `DATA_RESEARCH=READY` 只授权离线研究数据消费，不授权 selection、Cloud backtest、order、fill、paper、live 或 broker；
- `SHADOW_SELECTION=READY` 不授权 order 或 fill；
- `EXECUTION=READY` 仍不自动授权外部动作，只表示 canonical evidence gate 已满足；
- 不能把 manual Results SHA、derived artifact SHA 或 repository hash 标成 provider raw checksum；
- 不能静默排除 1 个 never-chain session、缩短 `2021-02-22..2025-12-02` 窗口或改变预期 1202 sessions；
- quote age、spread、min OI、min volume 继续为 `UNKNOWN_REQUIRES_POLICY_REVIEW`，本任务不填数值。

## 3. 分阶段检查集合

### 3.1 `DATA_RESEARCH`

必须满足：

- `cache_identity`
- `chain_presence`
- `engine_identity`
- `evidence_identity`
- `exchange_calendar_identity`
- `local_cache_dq_scope_separation`
- `open_interest_freshness`
- `prior_day_model_freshness`
- `provider_raw_checksum` 或第 4 节 exact alternate evidence route
- `quote_freshness`
- `quote_integrity`
- `symbol_mapping_identity`

### 3.2 `SHADOW_SELECTION`

必须先有 `DATA_RESEARCH=READY`，并额外满足：

- `signal_selection_chronology`

### 3.3 `EXECUTION`

必须先有 `SHADOW_SELECTION=READY`，并额外满足：

- `fill_forward_ambiguity`
- `order_fill_chronology`

所有 15 项仍必须在输入中恰好出现一次。阶段划分只决定 applicability，不删除检查，也不改变事实状态。

## 4. Platform-attested derived evidence alternate route

仅当 `provider_raw_checksum` 为 `NOT_EVALUATED / PROVIDER_RAW_CHECKSUM_UNAVAILABLE` 时，可以评估
alternate route。它必须同时绑定：

- provider 明确状态 `UNAVAILABLE_PROVIDER_DOES_NOT_EXPOSE`；
- derived evidence export classification 必须为 `EXPORT_ALLOWED_DERIVED`；raw option fields 仍只能是
  `QC_ONLY_NOT_EXPORTED` 或 `EXPORT_PROHIBITED`；
- platform、tier、engine、bundle 与 evidence manifest identity 均 confirmed；
- repository code SHA、2481 shared contract SHA、2482 policy SHA、evaluator SHA；
- requested start/end、expected/observed session count；
- canonical derived evidence content SHA-256 与 manifest content SHA-256；
- deterministic replay status `PASS`；
- license state 不是 unknown。

上述条件全部满足时，只能产生
`SOURCE_EVIDENCE_SATISFIED_BY_PLATFORM_ATTESTED_DERIVED_ROUTE`，不得把 2482 原始
`provider_raw_checksum` check 改写为 PASS。任一字段缺失、哈希漂移、session count 不符、identity 未确认或
license unknown，alternate route 必须 `NOT_SATISFIED`。

## 5. 实施范围

Task-owned：

- 本 supporting requirement；
- `config/research/qqq_options_staged_dq_pit_readiness_v1.yaml`；
- `src/ai_trading_system/qqq_options_research/staged_dq_pit_readiness.py`；
- `tests/test_qqq_options_staged_dq_pit_readiness.py`；
- `config/architecture/fragments/modules/qqq_options_staged_dq_pit_readiness.yaml`；
- `config/architecture/fragments/flows/qqq_options_staged_dq_pit_readiness.yaml`。

Coordinator-owned：

- canonical task registry/index 与 generated compatibility views；
- `docs/system_flow.md`；
- architecture/DevEx/compatibility/deprecation generated authority；
- `config/architecture/devx_006d_report_catalog_flow_authority.yaml` 的 exact system-flow source seal；
- `inputs/architecture/arch_004g_deprecation_inventory.yaml` 与对应 frozen-count regression；
- report/catalog/flow source-seal 与 canonical task-count frozen regressions；
- formal validation artifacts。

明确不修改：

- 2481 shared contract 和 2482 V1 policy/evaluator bytes；
- 2532 immutable execution package、raw Results 与 external counters；
- 2533 admission report/seals；
- QuantConnect project/API/browser、Cloud、Object Store、raw option rows；
- selection、engine、paper/live/broker、order 或 fill 行为。

## 6. 阶段与验收

### S0 — Registration and contract freeze

- canonical task registration 与本 requirement 完成；
- SINGLE_LANE contract-wave preflight PASS；
- exact stage/check mapping、alternate-route fields 与 safety boundary 冻结。

### S1 — Deterministic implementation

- exact-byte policy loader 与 hash binding；
- strict typed input 拒绝 extra fields、重复/缺失 check、bad hash、float、naive timestamp；
- readiness result 同时输出三个阶段，按 `READY / NOT_READY / BLOCKED` 单调传播；
- 2533 coverage replay 必须保持三个阶段 `BLOCKED`，原因包含 `chain_presence=FAIL`；
- raw checksum unavailable 只有 exact alternate route 完整时才满足 source-evidence 子条件；
- canonical JSON、self-excluding content seal、replay 和 tamper tests PASS。

### S2 — Integration and closeout

- system flow 与 architecture fragments 同步；
- focused 与 applicable Architecture、Contract、Integration、Reproducibility、Full 在 final tree PASS；
- task terminal、local-main ff-only、ordinary non-force push、SHA verification 与 branch/lease cleanup 完成；
- 后继 external diagnostic 必须从本任务 RELEASE exact main 独立登记，不得复用 2532 token。

## 7. 当前结论

本任务可以消除“用执行证据才能获准开始研究”的循环依赖，并为 provider raw checksum 不可得提供不冒充
raw checksum 的严格替代证据路径。它不会自行修复 1 个 never-chain session，因此本任务完成后当前研究仍
保持关闭；下一步才是针对该 session 的 export-safe provider/transport attribution。

## 8. 实施与预关闭证据

- exact-byte policy、typed evaluator、三阶段单调传播、platform-attested alternate route 与 tamper
  检查已实现；
- 2533 immutable admission replay 保持 `DATA_RESEARCH / SHADOW_SELECTION / EXECUTION = BLOCKED`，
  仍由 `chain_presence=FAIL` 阻塞；
- focused implementation tests：`12 passed`；2482/2533 邻接 contract：`57 passed`；
  architecture/governance focused：`72 passed`；
- pre-close Architecture：`865 passed in 363.47s`，artifact
  `outputs/validation_runtime/architecture-fitness_20260818T153504Z/test_runtime_summary.json`；
- pre-close Contract：`276 passed in 185.71s`，artifact
  `outputs/validation_runtime/contract-validation_20260818T154128Z/test_runtime_summary.json`；
- pre-close Integration：`995 passed / 642 warnings in 49.11s`，artifact
  `outputs/validation_runtime/integration_20260818T154452Z/test_runtime_summary.json`；
- 所有上述动作均为 `production_effect=none / broker_action=none`，未访问 Cloud、raw rows、订单、
  成交或账户数据；
- RELEASE 仍要求终态 task bytes、generated authority 与实现处于同一 final tree，并在该树完成五级
  formal validation、ff-only local main、ordinary non-force push 与 SHA verify。
- 首次 final-tree Full 为 `9205 passed / 7 failed / 3 skipped / 644 warnings in 1582.81s`，artifact
  `outputs/validation_runtime/full_20260818T160938Z/test_runtime_summary.json`；7 个失败均收敛到新增
  2534 尚未进入 Atlas page-effectiveness successor classification，触发
  `UNCLASSIFIED_SUCCESSOR_REVIEW_REQUIRED`。本任务保留该 failure evidence，并通过显式披露
  “staged readiness authority 已实现、当前 2533 evidence 仍 BLOCKED”修复 freshness contract；
  不删除 fail-closed 检查，也不硬写 `CURRENT`。
