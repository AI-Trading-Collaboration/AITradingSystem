# TRADING-2491：QQQ Options Cross-Layer Validation Harness V1

最后更新：2026-08-03

稳定任务 ID：`TRADING-2491_QQQ_OPTIONS_CROSS_LAYER_VALIDATION_HARNESS_V1`

优先级：`P1`

状态：`BASELINE_DONE`

mode：`SINGLE_LANE`

production effect：`none`

broker action：`none`

外部平台 Owner token：`NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS`

## 1. 目标

本任务建立 strictly offline、deterministic、content-bound 的 QQQ options cross-layer QA harness。它把
TRADING-2481–2490 已冻结的 public/canonical contract 当作只读 authority，统一管理 synthetic scenario specs、
golden identity、typed observation、validation report 与 cloud smoke checklist。

它解决三个问题：

1. valid、stale/crossed/missing quote、no-contract、partial/reject/insufficient cash、ITM expiry、corporate action
   等跨层案例有一个明确、完整、可 replay 的 fixture inventory；
2. fixture 与 predecessor schema/module/policy exact hashes 绑定，任何漂移都会使 golden fail closed，而不是静默改写；
3. 2492 cloud smoke 前置检查以 typed blocked checklist 表达，未获 Owner 授权时不能被 caller 自报 READY。

## 2. 非目标与安全边界

本任务不：

- 登录 QuantConnect、创建/修改 project、调用 API/CLI/HTTP/Object Store 或运行 cloud backtest；
- 下载、读取、复制、重建或导出 raw option chain、minute quote、OpenInterest、Greeks 或 provider raw rows；
- 激活 2485 selection、2486 execution、2487 accounting、2488 lifecycle 或 2490 reconciliation policy；
- 修改 2481 shared records、2482 DQ/PIT/cache identity、2483 package、2484 adapter 或 2485–2490 public boundary；
- 用 synthetic PASS 替代真实 platform evidence、license review、DQ/PIT、reconciliation 或 Owner stage gate；
- 为 2492 填写 window length、order cap、resource cap、tolerance、latency、slippage、fee、spread、OI、volume、
  DTE、moneyness、delta 等未 reviewed 数值；
- 形成策略有效、range expansion、promotion、paper/live/production 或 broker authorization 结论。

固定 safety：research-only；external platform/cloud/API/CLI/HTTP/Object Store/raw export/paper/live/production/broker
全部 false/none；`synthetic_fixture_is_platform_evidence=false`；`synthetic_pass_may_authorize_pilot=false`。

## 3. Exact inherited authority

2491 policy 必须列出并在 loader 中重算 2481–2490 contract/policy/module 文件的 LF-normalized SHA-256。
任何缺失、symlink、path escape、hash drift 或 duplicate authority id 一律 fail closed。2491 只调用 predecessor
`seal`、`from_json_bytes`、`canonical_bytes`、`canonical_sha256`/`content_sha256` authority；不得复制 shared record。

必须继续保留：

- 2481 shared contract/schema 与 shared safety envelope；
- 2482 local cached DQ、option-event DQ、PIT/stage 分轴，UNKNOWN/NOT_EVALUATED 永不产生 PASS；
- 2483 primary requested/evaluated start=`2021-02-22` 与 derived/export-safe package；
- 2484 no cloud/no raw export adapter boundary；
- 2485 tracked selection unauthorized；
- 2486 no daily-close/same-bar fill 与 unresolved reality-model numeric policy；
- 2487 Decimal cash/premium/fee/lot identity 与 no negative/short cash safety；
- 2488 expiry/exercise/assignment/corporate-action scope invalidation；
- 2489 strict evidence bundle/no caller-ready boundary；
- 2490 exact/numeric reconciliation、seven-class taxonomy 与 external PASS 不覆盖 internal FAIL。

## 4. Fixture corpus and golden authority

tracked policy：`config/research/qqq_options_cross_layer_validation_harness_v1.yaml`。

tracked golden manifest：`config/research/qqq_options_cross_layer_validation_golden_v1.yaml`。

fixture inventory 精确十项：

1. `VALID_CROSS_LAYER_SYNTHETIC`；
2. `STALE_QUOTE_REJECTED`；
3. `CROSSED_QUOTE_INVALID`；
4. `MISSING_QUOTE_INVALID`；
5. `NO_ELIGIBLE_CONTRACT_CASH`；
6. `PARTIAL_FILL_CANCELED`；
7. `VENUE_REJECTED_CASH`；
8. `INSUFFICIENT_SETTLED_CASH`；
9. `ITM_EXPIRY_SCOPE_INVALID`；
10. `CORPORATE_ACTION_SCOPE_INVALID`。

每个 spec 必须包含：scenario id、stimulus class、terminal layer、expected status/reason codes、order/fill count、
cash preservation、run validity、DQ/PIT status、required artifact roles、synthetic-only caveat 与 canonical
`fixture_sha256`。golden manifest exact-bind policy hash、十个 fixture hashes 与 aggregate corpus hash。

`fixture_sha256` 只对不含自身 expected hash 的 canonical semantic payload 计算，避免 caller-supplied/self-referential
identity。schema/module/policy hash 变化、scenario 缺失/新增/重复、顺序漂移或 expected semantic drift 都会使 golden
验证失败；历史 golden 不自动重写。

## 5. Typed observation and report

task-owned public contract 包括：

- policy/authority/scenario/golden/load-result models；
- sealed `QQQOptionsCrossLayerObservation`：actual terminal layer/status/reasons、order/fill counts、cash/run/DQ/PIT、
  canonical predecessor artifact bindings 与 synthetic evidence classification；
- sealed per-scenario validation 与 aggregate report；
- sealed cloud smoke checklist 与 item records；
- typed `QQQOptionsCrossLayerValidationError`；
- policy/golden loader、fixture corpus builder、observation validator、aggregate report builder、checklist builder 与 replay API。

所有 task-owned sealed records 使用 strict/extra-forbid/frozen models，提供 `seal`、canonical bytes/hash 与
`from_json_bytes`；拒绝 noncanonical JSON、caller hash、NaN/Inf、naive/future timestamp、unsorted/duplicate tuple、
unknown scenario、missing/extra observation、artifact hash/schema/path mismatch 与 input permutation drift。

observation 只能代表 synthetic QA 事实；即使 aggregate report PASS，`investment_interpretation_allowed=false`、
`pilot_authorized=false`、`range_expansion_allowed=false`。

## 6. Required DQ/PIT and research window

- primary requested/evaluated start exact=`2021-02-22`；`2022-12-01` 不是 default；
- 本任务不读取 cached market/macro data，因此不新跑 `aits validate-data`，但也不得生成真实 DQ PASS；
- synthetic DQ/PIT PASS 仅说明 fixture 的期望状态被覆盖，classification 必须为
  `SYNTHETIC_TEST_ONLY_NOT_PLATFORM_EVIDENCE`；
- FAIL/NOT_EVALUATED fixture 必须保留原值，aggregate harness PASS 不能把它洗白；
- report 同时披露 synthetic fixture coverage status 与真实 platform evidence status，后者固定
  `NOT_EVALUATED_NO_AUTHORIZED_PILOT`。

## 7. Cloud smoke checklist

checklist 精确覆盖 Owner token、capability receipt、primary window、exact code/project identity、subscription/engine、
resource boundary、no raw export、six result mappings、two-person attestation、DQ/PIT、local reconciliation、stage-gate
handoff。2492 尚未授权时：

```text
status=BLOCKED_OWNER_AUTHORIZATION
owner_authorization_token=NOT_GRANTED_FOR_EXTERNAL_PLATFORM_ACTIONS
external_action_executed=false
all items=PENDING_OWNER_AUTHORIZATION or NOT_EVALUATED
```

caller 不能提供 token 或把 item 改成 PASS；2491 只生成 checklist，不执行检查项对应的外部动作。真实授权与
preregistered numeric scope 必须由 2492 独立 tracked policy/Owner decision 接管。

## 8. Verification matrix

focused tests 至少覆盖：

1. tracked policy/golden exact、十项 inventory、primary window 与 safety；
2. 2481–2490 authority path/hash replay、missing/drift/symlink/path escape/duplicate negatives；
3. 每项 fixture semantic payload 与 expected golden hash；
4. aggregate corpus identity、permutation determinism 与 canonical replay；
5. valid observation PASS；每个 negative fixture 的 terminal layer/status/reason/count/cash/run/DQ/PIT exact；
6. missing/extra/duplicate scenario、wrong reason/layer/status/count/DQ/PIT/artifact binding FAIL；
7. schema/module/policy drift 明确使 golden invalid；
8. aggregate PASS 不把 synthetic DQ/PIT 变成 platform evidence，不授权 pilot/range/investment；
9. cloud checklist 默认 blocked、forged token/READY/item PASS 不可注入、external actions=none；
10. adjacent 2480–2491、compatibility/deprecation、DevEx/task shadow 与 final-tree five-tier validation。

## 9. Governed stages and path claims

frozen base：`0b0b6be06b58023ee3c7afbf132b9b6b30959bfe`

branch：`codex/trading-2491-cross-layer-validation-harness`

`contract_change=true`：新增 task-owned QA public contract/policy/golden，不改变 2481–2490 shared/public authority。

|阶段|工作|退出条件|
|---|---|---|
|S0|task row、requirement、START/LANE|exact base、claims、contract-change、lease PASS。|
|S1|policy、authority binding、fixture/golden|十项 corpus 与 predecessor drift fail closed。|
|S2|observation/report|typed exact validation、canonical replay、property/golden PASS。|
|S3|cloud checklist|default blocked、no forged authorization、no external action。|
|S4|shared wiring/closeout|system flow、generated/current authority、adjacent/compat/formal、ordinary push/cleanup PASS。|

task-owned paths：

```text
config/research/qqq_options_cross_layer_validation_harness_v1.yaml
config/research/qqq_options_cross_layer_validation_golden_v1.yaml
src/ai_trading_system/qqq_options_research/cross_layer_validation.py
tests/test_qqq_options_cross_layer_validation.py
docs/requirements/TRADING-2491_QQQ_Options_Cross_Layer_Validation_Harness_V1.md
config/architecture/fragments/flows/qqq_options_cross_layer_validation_harness.yaml
config/architecture/fragments/modules/qqq_options_cross_layer_validation_harness.yaml
```

coordinator-owned paths：

```text
docs/task_register.md
docs/system_flow.md
inputs/architecture/**
registry/development_tasks_shadow/**
registry/development_tasks_shadow_v2/**
tests/test_arch_004_refactor_policy.py
tests/test_arch_004g_deprecation.py
tests/test_trading2452_architecture_contract.py
```

known-unrelated exclusion `docs/research/growth_tilt_owner_diagnosis_pack.md` 不得读取、hash、copy、stage 或修改。

## 10. Status record

- 2026-08-03：2490 ordinary main push/cleanup 后，从 exact latest main
  `0b0b6be06b58023ee3c7afbf132b9b6b30959bfe` 登记 2491。首写仅限 task row 与本 requirement；
  START/LANE PASS 前不写 policy/golden/module/tests/fragments/shared generated paths。外部 QuantConnect 动作=none。
- 2026-08-03：START/LANE PASS 后完成 policy/golden/module/tests、architecture fragments 与 system flow；focused
  `39 passed`，2480–2491 adjacent `320 passed`，Ruff/mypy/compileall PASS。DevEx 重建并验证为
  `1080 modules / 1247 tests / 856 writers / 0 violations`，task shadow 为 `961/456/505` 且 byte-identical。
  compatibility/deprecation 与 final-tree formal five-tier evidence 在同一 frozen candidate 收口；任何失败必须保留
  failure-fix parent，不能用旧树证据替代。外部平台动作继续为 none。
