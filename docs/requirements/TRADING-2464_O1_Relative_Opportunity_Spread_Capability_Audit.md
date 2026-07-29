# TRADING-2464：O1 Relative Opportunity Spread Capability Audit

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT`

优先级：`P0`

状态：`CLOSED_INSUFFICIENT_COVERAGE_OR_DQ`

production effect：`none`

broker action：`none`

## 1. 目标与当前授权

本任务规划如何验证冻结 target：

```text
RELATIVE_OPPORTUNITY_SPREAD
= QQQ_FORWARD_TOTAL_RETURN - SGOV_FORWARD_TOTAL_RETURN
```

唯一允许回答的问题是：在 `TRADING_2463_O1_S4_PILOT_V1` 下，受治理历史输入是否具有
可测、跨 fold、跨 regime/event、优于 train-only simple baseline 且经 mandatory
falsification 后仍成立的 relative-opportunity capability。

2026-07-29，Owner 要求参考网页 `GPT-5.6 Pro` exact-commit 审阅规划后续任务。该要求授权
记录本计划，不等于授权读取新的真实 coverage/result、训练模型或启动 capability audit。
启动前仍需新的显式 Owner decision。网页回答是 advisory planning evidence，不是仓库政策、
模型身份或后端路由 authority。

2026-07-30，Owner 以
`owner_decision:TRADING-2464:2026-07-30:approve_o1_m1_ridge_cross_asset_state_single_family_v1`
选择决策包 A。该决定授权 exact 复用 reviewed
`M1_RIDGE_LINEAR + CROSS_ASSET_STATE`，并按固定顺序进入 serial contract freeze、
synthetic builder/validator、isolated DQ、coverage-only gate 与至多一次 canonical run；
它不允许跳过任一前置门禁，也不授权任何下游投资或生产行为。

## 2. 权威输入与已经解除的 blocker

权威输入：

- `TRADING_2463_O1_S4_PILOT_V1` reviewed preregistration freeze；
- primary research window 起点 `2021-02-22`；
- D0B2B strict receipt
  `dq_execution_28af63a1e747ba675e17d3001d8028592b6ec0ef63e823bcfa9463889b0cb5c4`；
- receipt status=`PASS`、error_count=`0`、warning_count=`0`；
- requested window=`2021-02-22..2026-07-27`；
- evaluated window=`2021-02-22..2026-07-24`；
- TRADING-2461/2462 的负面历史只作为不可继承和 contamination 边界。

D0B2B strict PASS 已解除“canonical DQ 尚未 strict PASS”的数据前置 blocker。D0E
`daily_score_daily@1.0.0` same-store migration 只证明该 exact consumer；不得把它冒充为
O1 research consumer、O1 capability evidence 或 O1 Owner authorization。

## 3. 明确禁止范围

本任务不得：

- 修改 O1 target、continuous form、5 common sessions horizon、split、coverage floors、
  primary metric、numeric gates 或 mandatory falsification；
- 在真实 count/result 可见后选择或增加 model/feature family；
- 增加 sensitivity horizon、O2/O4、QLD、TQQQ 或旧 tail-risk evidence 补救 O1；
- 访问 prospective；
- 创建 Decision Value Audit、risk overlay、candidate family、strategy backtest、weights、
  QLD selector、paper-shadow、promotion、production 或 broker action；
- 把 capability skill 解释为 investment value 或 action authorization。

## 4. Owner Gate 与前置 policy gap

启动所需 Owner decision 建议稳定为：

```text
owner_decision:TRADING-2464:<DATE>:authorize_o1_relative_opportunity_spread_capability_audit_v1
```

Owner 授权必须绑定 execution exact base、S4 policy bytes、input commitment、model/feature
family authority、allowed runtime 和 evidence-lineage claims。没有该 decision 时保持
`BLOCKED_OWNER_INPUT`。上述 Owner A decision 已收到，因此当前只解除 model/feature family
选择门，进入结果读取前的 serial contract freeze；真实 coverage 与模型运行仍受 active
contract、isolated strict DQ 和 synthetic validation 门禁约束。

S4 冻结文件禁止未来 audit 新增 model family，但没有在该文件中给出可直接执行的 exact
model/feature family identity。任何真实 count/result 前必须解析已有 reviewed authority；
若不存在，则输出：

```text
BLOCKED_POLICY_GAP_MODEL_FAMILY_NOT_FROZEN
```

只能在结果读取前由 Owner 批准 policy addendum/new version；实现者不得自行选择模型。

2026-07-30，Owner 要求继续推进。该指令被解释为授权审计当前状态、准备 serial
contract wave 和 Owner 决策材料，不被扩大解释为替 Owner 选择 exact model/feature
family。已新增
`docs/requirements/TRADING-2464_O1_Model_Feature_Family_Owner_Decision_Pack.md`
与 inactive proposal
`config/research/o1_relative_opportunity_capability_audit_v1_proposal.yaml`。推荐显式复用
`M1_RIDGE_LINEAR + CROSS_ASSET_STATE`，但在 Owner 选择前仍为
`OWNER_REVIEW_REQUIRED_NOT_ACTIVE`。

同时发现 D0B2B authority 记录的 exact receipt bytes 当前不在主工作区或三个已登记
worktree。需求文字、receipt ID 与 expected SHA 不能替代执行输入；必须恢复并 hash-verify
exact bytes，或对新的 canonical strict PASS receipt 发布 reviewed policy version 后，才能
读取真实 coverage。

后续只读审计确认 OPS-070 permanent runtime clone
`D:\Work\AITradingSystem_ops_runtime` 保留 receipt、authorization、pointer、transaction
及三份 immutable input members，全部 exact hash 与现有 historical-acceptance validator
均 `PASS`。因此 evidence 可恢复，但尚未 materialize。当前 main/runtime live
`data/raw` 已与 receipt input bytes 不同；不得只复制 receipt 或覆盖 live projection。
Owner 选择 A（或批准完整 B）后，才允许通过既有 isolated-candidate 路径 materialize 并
重新执行 strict validation。

## 5. 执行拓扑与任务波次

推荐拓扑：

```text
SERIAL_CONTRACT_WAVE
  -> RESTRICTED_DUAL_LANE
  -> SINGLE_LANE_CANONICAL_RUN_AND_CLASSIFICATION
```

### Wave 0：任务与 Owner Gate

- 固定 execution exact base 和 task-owned/coordinator-owned paths；
- 绑定 S4 policy、D0B2B receipt 与 source/input identities；
- 建立 no-result-read attestation；
- 取得 Owner authorization；
- 未授权或 policy identity 缺失时停止，不做真实计算。

### Wave 1：Serial Contract Freeze

在真实 count/result 前唯一冻结：

- target/label/available-at/maturity；
- 5-session fold/purge/embargo schedule；
- DQ/PIT/calendar/source/publication/cache identity；
- exact model/feature family；
- event ledger 与 append-only attempt ledger；
- seed/runtime/software identity；
- mechanical capability classification mapping。

shared schema、DQ/PIT、cache/publication identity 与 model interpretation 必须由 coordinator
串行冻结，不能由 engineering 与 strategy-evidence 两线分别定义。

### Wave 2：Deterministic Builder 与 Independent Validator

只使用 synthetic/controlled fixtures：

- 重建 label、maturity、split、purge、embargo、train-only transform/baseline；
- 覆盖 future timestamp、test-data fitting、missing common session、receipt drift、
  row-order/result tamper 等负例；
- 同输入双构建必须 byte-identical；
- validator 必须从 source 重建 split、prediction、metric、falsification 和 report。

失败结论是 `ENGINEERING_VALIDATION_FAILED`，不得写成 O1 研究失败。

### Wave 3：Coverage-only Eligibility Audit

只允许读取真实 eligibility/count，不训练模型：

- matured/eligible/purged/embargoed/missing counts；
- per-fold effective sample、autocorrelation ESS 与 non-overlap equivalent；
- regime cells、event episodes 与 final partial fold coverage；
- receipt/publication/panel exact identity。

任何 mandatory floor 不足，机械输出 `INSUFFICIENT_COVERAGE_OR_DQ` 并关闭；不得缩短 horizon、
合并 fold/cell、删除 regime 或事后增加 event。

### Wave 4：单次 Canonical Capability Run

仅在 Waves 0～3 PASS 后，运行一个 frozen historical-seen-only experiment：

- one canonical model/feature family；
- train-only preprocessing 与 unconditional-mean baseline；
- OOF primary metric、moving-block bootstrap 与 supporting diagnostics；
- 不做自动搜索、结果导向 transformation 或新增模型。

### Wave 5：Mandatory Falsification

必须执行 exact reconstruction、feature timing lag、purge/embargo stress、fold
jackknife/influence、regime/event concentration、autocorrelation-preserving placebo、
target-boundary perturbation、simple-baseline increment、multiple-testing 与 DQ/lineage
closure。任何 mandatory axis 的 PASS 不得抵消另一 axis 的 FAIL/INSUFFICIENT。

### Wave 6：Mechanical Classification 与 Owner Closeout

最终 class 只允许：

- `MEASURABLE_RELATIVE_OPPORTUNITY_SKILL`
- `NO_MEASURABLE_SKILL`
- `INSUFFICIENT_COVERAGE_OR_DQ`
- `INSUFFICIENT_ROBUSTNESS_EVIDENCE`

leakage、attempt contamination、lineage tamper 或 exact reconstruction mismatch 使 audit
invalid，不产生 capability class。正面 class 只允许 Owner 决定是否另立 Decision Value
Audit；不能穿透任何下游 gate。

## 6. Lane Claims

serial contract wave 完成后才允许受限双线：

Engineering lane 可拥有 synthetic fixtures、builder/validator、negative tests、
deterministic serialization 与无真实结果的 CLI/preflight。

Strategy-evidence lane 可拥有 exact source inventory、DQ/PIT/publication commitment、
immutable event ledger 与经授权的 coverage-only run；不得改代码、policy 或 shared schema。

Coordinator-only：

- task register 与本 requirement；
- `docs/system_flow.md`（仅在实际数据流变化时更新）；
- policy/schema、DQ/PIT/cache identity；
- event/attempt/report/artifact registries；
- generated manifests、formal validation 与 final classification。

实际 prediction、mandatory falsification 和 classification 必须回到一个 exact tree、一个
run identity 和一个 attempt ledger。

## 7. Evidence-lineage 与验收标准

正式 run 至少绑定：

- review snapshot/execution base SHA；
- policy path/blob/content SHA；
- Owner decision ID；
- DQ receipt path/ID/SHA/profile/as-of；
- publication generation/manifest 与 QQQ/SGOV exact file SHA/rows/range；
- calendar/common-session identity；
- label/feature/model/preprocessor source SHA；
- split/purge/embargo、event 与 attempt ledgers；
- seed/runtime/package lock/CLI argv；
- prediction/metric/falsification/report/envelope/run-ledger SHA；
- independent validator result；
- `prospective_accessed=false`、`production_effect=none`、`broker_action=none`。

验收要求：

1. 新 Owner authorization 存在且 exact 绑定；
2. model/feature family 在任何真实 count/result 前有 reviewed authority；
3. canonical DQ strict `PASS/0/0` 与本 audit exact input identity 一致；
4. label、available-at、maturity、common sessions 可逐行重建；
5. coverage floors 全部 PASS，否则 early stop；
6. frozen primary gates与 mandatory falsification机械执行；
7. append-only attempt ledger 完整；
8. independent validator 0 errors；
9. 结果只取四个允许 class；
10. 所有下游 enablement 仍为 false。

## 8. 当前执行点

- `status=CLOSED_INSUFFICIENT_COVERAGE_OR_DQ`
- `serial_contract_freeze=PASS_AUTHORITY_148_ARCHITECTURE_789_CONTRACT_276`
- `synthetic_builder_validator=PASS_SYNTHETIC_ONLY_DETERMINISTIC_INDEPENDENT_RECONSTRUCTION`
- `data_prerequisite=ISOLATED_CANDIDATE_STRICT_DQ_PASS_0_0`
- `owner_authorization_for_capability_audit=true`
- `owner_option=A_ADOPT_M1_RIDGE_CROSS_ASSET_STATE_EXACT_REUSE`
- `model_feature_family_authority=OWNER_APPROVED_BEFORE_RESULT_READ`
- `model_feature_family_proposal=SUPERSEDED_BY_ACTIVE_CONTRACT_FREEZE`
- `required_dq_receipt_bytes_present=true_in_retained_candidate`
- `dq_exact_chain_recoverable_from_ops_runtime=true`
- `dq_isolated_candidate_materialized=true`
- `dq_gate_id=o1_dq_gate_60926d9b01e451af07a77fe8fdf209e2`
- `dq_gate_sha256=ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`
- `fresh_dq_receipt_id=dq_execution_d80529d1c713fee5f8602830912c14c2bdca64a59c64d943fbedd7c044d677cb`
- `fresh_dq_receipt_sha256=6f37031a57b363189862b63a6bab396ff33b5f678b37bbb34ff4261be55ebe08`
- `fresh_dq_window=requested_2021-02-22_to_2026-07-27_evaluated_to_2026-07-24`
- `official_source_acquisition_completed=true`
- `official_source_request_count=179_all_http_200_and_checksum_bound`
- `initial_event_source_gate=BLOCKED_PARSER_DEFECT_NOT_EMPIRICAL_COVERAGE`
- `initial_event_source_gate_id=o1_event_attempt_gate_596130e1136f86dc2f3c8918f7b80f4c`
- `retained_raw_primary_source_files=179`
- `retained_raw_primary_source_bytes=160417518`
- `raw_replay_required=true`
- `raw_refetch_allowed=false`
- `event_attempt_freeze_gate_status=PASS_EVENT_AND_ATTEMPT_LEDGERS_FROZEN`
- `event_attempt_freeze_gate_id=o1_event_attempt_gate_06fd958a1529c17c31b804ebd0307632`
- `event_attempt_freeze_gate_sha256=5855720e4f0d5f97748730d5f90cbba18d852c5c90c67197229832c3d51faba5`
- `event_ledger_id=o1_event_ledger_9969f27e7c578b658c500c2f3b71a610`
- `event_ledger_sha256=d714af0779e6edb97f9ed143192c7b8858e70f21196e96be190b837cc0deb476`
- `event_count=171_fomc_43_cpi_64_nfp_64`
- `real_coverage_read_allowed_now=false_single_run_consumed`
- `coverage_runner_implementation=PASS_SYNTHETIC_CONTROLLED_FIXTURES_ONLY`
- `coverage_audit_executed=true`
- `coverage_source_commit=1bf9fb13245064ec2a505ea864e2e127ad445d41`
- `coverage_report_id=o1_coverage_report_9b5708c6c36ac69cc7355fee8567a953`
- `coverage_report_sha256=bbed79b499b57274dd49bede0c37219894233964732fcde5656626933781ada7`
- `coverage_gate_id=o1_coverage_gate_b240158b3b7d3211ad51852217aa6d93`
- `coverage_gate_sha256=a97ee44832a41aeb90a6f9a18b0358eb81cefec4d491438deb6fd27b624f31b8`
- `coverage_failed_checks=F01_train_98_lt_100;F02_test_23.71930136737_lt_24;volatility_high_2_folds_lt_3;current_drawdown_low_13_lt_15`
- `mechanical_classification=INSUFFICIENT_COVERAGE_OR_DQ`
- `new_results_read=true_coverage_only`
- `prospective_accessed=false`
- `model_training_executed=false`
- `decision_value_audit_started=false`
- `risk_overlay_created=false`
- `candidate_backtest_weights_created=false`
- `qld_automatic_selection_enabled=false`
- `production_effect=none`
- `broker_action=none`

### 8.1 Isolated candidate 生命周期登记

- owner task：`TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT`
- exact output root：
  `D:/Work/AITradingSystem/outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z`
- candidate project root：上述目录下 `candidate_project/`
- source root：`D:/Work/AITradingSystem_ops_runtime`，只读，禁止任何 mutation；
- purpose：从 frozen immutable transaction materialize candidate-specific publication，
  逐对象校验，并运行 fresh canonical strict DQ；
- creation precondition：DQ runner focused validation PASS，且 output root 不存在；
- retention：若 strict DQ PASS，保留到 coverage-only 与可能的单次 canonical run 均完成，
  因为后续必须复用同一 candidate/store identity；
- exit condition：所需 receipt、copy manifest、coverage/run evidence 已进入 canonical governed
  artifact location并核验 hash，且没有 active process、scheduler 或后续 acceptance 依赖；
- cleanup：退出条件满足后，先审计 tracked/untracked/ignored unique evidence，再按 exact absolute
  allowlist 删除该 root；若失败或中止，必须在本任务文件记录风险、next owner 与新 exit condition。

### 8.2 Event / attempt freeze workspace 生命周期登记

- exact root：
  `D:/Work/AITradingSystem/outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_event_attempt_freeze_v1`
- raw primary-source root：上述目录下 `raw_primary_sources/`；只允许 Federal Reserve
  `federalreserve.gov` 与 BLS `bls.gov` 的 official archive bytes；
- governed outputs：`event_source_manifest.json`、`event_ledger.json`、
  `attempt_ledger.json` 与 `event_attempt_freeze_gate.json`；
- purpose：在任何 eligibility/coverage count 读取前，冻结 FOMC/CPI/NFP release
  occurrence timestamps、source/known-at/available-at lineage，以及唯一
  `O1_M1_RIDGE_CROSS_ASSET_STATE_V1` attempt family；
- creation precondition：ledger runner focused validation PASS、exact root 不存在、
  existing isolated DQ gate SHA 仍为
  `ca02b4310f99d664bb8d987debd4900f4367935b3938663c7a633400d988a1ca`；
- source discipline：每个 HTTP response 记录 endpoint、request parameters、download
  timestamp、status、byte size 与 SHA-256；event row 必须能追溯到一个保存的 exact
  official response，不得使用 current-view 推断未知 historical schedule-known-at；
- acquisition boundary：2026-07-30 登录态 Chrome 预检可以读取 BLS archive/PDF，但
  browser-control 下载接口没有向项目暴露可复验的原始 PDF 路径；早期 CLI 探测曾返回
  HTTP 403，因此 runner 仍必须以实际 response 为准。发布后 exact runner 的唯一正式采集
  共完成 179 个 official HTTP 200 response，逐项保存 bytes、size 与 SHA-256。浏览器可见性
  仍不能替代 official bytes、checksum 与 source manifest；
- first-run preservation：首次正式采集生成的
  `event_source_manifest.json`、`attempt_ledger.json` 与
  `event_attempt_freeze_gate.json` 作为 immutable failure evidence 保留。它们分别绑定
  source commit `9105dcb32fa4029ca9770f264f4072045b0c5932`、179 份 raw official
  response、唯一 attempt family 与 blocked gate，不得覆盖、删除或改写；
- parser defect finding：CPI 64/64、FOMC 43/43 release 解析通过；NFP 64/64 official
  页面均在 `embargoed until` 与 `8:30 a.m. (ET)` 之间插入 `USDL-xx-xxxx` release
  identifier。现有 parser 错误要求时间紧随 `until`，因此
  `O1_EVENT_BLS_TIMESTAMP_MISSING` 是工程缺陷，不是 empirical coverage/DQ 结论；
- replay correction：只能在同一 exact root 内，从 retained manifest 逐项复验
  path/size/SHA-256/domain/status 后离线重放；禁止重新下载、创建第二 workspace、覆盖首次
  工件或读取 coverage/result。修复必须接受有界 BLS release identifier、校验 BLS 正文日期
  与 archive URL 日期一致，并生成新命名的 superseding replay manifest/gate；原始 attempt
  ledger 继续约束同一个 family，不得借 replay 增加或更换模型/特征 family；
- retention：与 candidate 一同保留到 coverage/canonical/final classification closeout；
  coverage 只能读取 freeze gate 精确绑定的 ledger bytes；
- exit condition：event/attempt/coverage/run evidence 已转入 canonical governed artifact
  location并逐 byte 核验，且无 active process 或后续 acceptance 依赖；
- cleanup：与 8.1 使用同一最终 absolute-root allowlist；失败或 source 不足时保留已下载
  primary bytes与 blocker manifest，不创建第二目录。

### 8.3 Coverage-only output 生命周期登记

- exact root：
  `D:/Work/AITradingSystem/outputs/validation_runtime/trading_2464_o1_dq_20260729T183000Z/o1_coverage_only_v1`；
- owner task：`TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT`；
- purpose：从发布后的 exact runner commit 逐 SHA 复验 active policy、strict DQ gate、
  candidate 三份 immutable input、replay source manifest/event ledger/attempt ledger/gate，
  然后只计算 eligibility、fold/ESS、fold-train-only regime tertile 与 event-episode coverage；
- creation precondition：coverage runner focused/formal validation PASS、commit 已 fast-forward
  至 local/remote `main`、exact root 不存在、source commit 等于运行时 Git HEAD；
- single-run boundary：runner 不提供 resume、overwrite、second-candidate 或结果导向重跑；
  gate 为 PASS 或 `INSUFFICIENT_COVERAGE_OR_DQ` 都使该目录 immutable；
- governed outputs：`coverage_report.json` 与 `coverage_gate.json`。报告不保存逐行 target、
  feature、prediction、metric 或模型参数；非 common-session event 只记为 missing，不平移到
  相邻交易日；
- authorization boundary：coverage PASS 也只把
  `canonical_policy_update_eligible=true`；`model_training_allowed_now=false` 与
  `canonical_run_allowed_now=false` 继续保持，必须另做 exact evidence serial binding 后才可
  启动 canonical runner；
- retention：与 8.1/8.2 的 exact candidate/event evidence 一同保留到任务机械分类收口；
- exit condition / cleanup：coverage/canonical/final evidence 已转入 canonical governed
  artifact location并逐 byte 核验，且无 active process、scheduler 或后续 acceptance 依赖；
  满足后按 8.1 的同一 absolute-root allowlist 审计并清理，不能单独删除本目录来破坏证据链。
- current retained evidence：唯一目录已由 published commit
  `1bf9fb13245064ec2a505ea864e2e127ad445d41` 创建；`coverage_report.json` 与
  `coverage_gate.json` 已逐 byte 绑定到 active policy。因 candidate、official source 与
  coverage failure chain 仍是最终机械结论的唯一复验证据，本轮保留整个 8.1 absolute root，
  不单独清理子目录。

## 9. 进度记录

- 2026-07-29：根据 Owner 要求建立后续规划。网页 `GPT-5.6 Pro` 回答成功读取公开仓库
  exact commit 并提出 serial-contract、coverage-only early-stop 和单次 canonical run；
  本文件仅采纳与仓库 S4/DQ/Owner 边界一致的内容。网页 UI、自述或回答质量均不构成后端
  路由、研究政策或 capability evidence。
- 2026-07-30：Owner 要求继续推进；完成 read-only checkout/lease/base audit，并在未读取
  新 O1 count/result 的前提下形成 model/feature family 决策包与 inactive proposal。
  推荐 exact 复用 reviewed `M1_RIDGE_LINEAR + CROSS_ASSET_STATE`，但不得把实现者推荐写成
  Owner approval。另发现 expected D0B2B receipt bytes 不在当前已登记 worktree，故真实
  coverage 还有独立 evidence-byte gate。
- 2026-07-30：扩大只读恢复审计后，在 OPS-070 permanent runtime clone 找到 exact
  receipt 与 immutable publication chain；receipt、authorization、pointer、transaction
  和三份 member 全部 SHA 验证通过，现有 historical-acceptance contract validator
  `PASS`。因 live `data/raw` 已后移，禁止只复制 receipt；等待 Owner 选择后才可在 isolated
  candidate materialize/revalidate。
- 2026-07-30：Owner 选择 A，授权 exact 复用
  `M1_RIDGE_LINEAR + CROSS_ASSET_STATE`。任务转入 serial contract freeze；当前 wave 只记录
  decision、exact base、S4、DQ transaction、model/feature 与 no-result-read 边界，尚未
  materialize、重跑 DQ、读取 coverage/result 或训练模型。
- 2026-07-30：serial contract freeze 已通过 focused contract `10/10`、
  authority/deprecation `148/148`、`architecture-fitness 789/789` 与
  `contract-validation 276/276`。下一步只能从该 wave 集成后的 exact `main` 基线实现
  synthetic builder/validator；真实 DQ、coverage 与模型训练仍未获执行许可。
- 2026-07-30：在 serial contract 集成 commit `b346aaa622de1c9671527fda4b89b84f2c08ac83`
  上完成 `SYNTHETIC_FIXTURE_ONLY` builder/validator：按独立公式重建 5-session O1 label、
  reviewed 28-feature cumulative prefix、purged/embargo fold membership 与 event episode
  window；deterministic double-build、authority/input/tamper 负例和既有 model-ladder 回归
  focused `9/9 PASS`。该波次没有读取真实数据、coverage 或 result，也没有训练模型。
- 2026-07-30：在 exact base `bca4cddddf5bd266f96b828f09423d015605c43b`
  实现 O1 专用 isolated DQ 编排层。它复用 reviewed candidate materializer 与 canonical DQ，
  但在 strict DQ 后立即停止，不生成 daily consumer authorization、不 dispatch consumer、
  不读取 coverage、不训练模型。runner/recovery/scope/tamper 聚焦验证为 `11/11 PASS`。
- 2026-07-30：唯一已登记候选从 OPS-070 exact historical transaction 成功物化，三份
  immutable member 逐对象 SHA/size 复验通过；fresh receipt
  `dq_execution_d80529d1c713fee5f8602830912c14c2bdca64a59c64d943fbedd7c044d677cb`
  为 strict `PASS/0/0`，requested `2021-02-22..2026-07-27`，evaluated
  `2021-02-22..2026-07-24`。首次进程在 DQ 完成后因摘要读取错误访问不存在的
  invocation `.value` 字段而中止；未创建第二候选、未重跑 DQ。修复为解析 canonical
  `value_json` 后，使用新增 fail-closed resume 路径复验唯一 copy manifest、publication、
  candidate objects、receipt 与 source inventory，并仅补写 gate。该中断不计作第二次
  empirical attempt；当前候选继续按 8.1 保留，下一步仅允许冻结 event/attempt ledger，
  coverage 与训练仍关闭。
- 2026-07-30：event/attempt runner 已完成实现并通过首轮 parallel focused `8/8`。runner
  先复验 exact DQ gate，再冻结唯一
  `O1_M1_RIDGE_CROSS_ASSET_STATE_V1` append-only attempt family；只接受
  `federalreserve.gov`/`bls.gov` HTTPS，保存每个官方响应并绑定 status/size/SHA-256。
  任一 mandatory family 的 index、release discovery、release bytes 或 timestamp 失败，
  都只生成 blocker source manifest、attempt ledger 与 blocked gate，不生成 event ledger，
  不授权 coverage、canonical run 或模型训练。真实 source acquisition 只能从该 runner
  集成后的 exact commit 执行，避免未提交代码成为 evidence authority。
- 2026-07-30：runner wave 正式验证通过：authority/deprecation=`151/151`，
  Architecture=`792/792`（runtime artifact
  `outputs/validation_runtime/architecture-fitness_20260729T192754Z/test_runtime_summary.json`），
  Contract=`276/276`（runtime artifact
  `outputs/validation_runtime/contract-validation_20260729T192955Z/test_runtime_summary.json`）。
  当前只允许完成 task-branch commit、local-main fast-forward 与 ordinary push；真实 source
  acquisition 必须从发布后的 exact commit 在同一已登记 root 执行。
- 2026-07-30：从已发布 exact commit
  `9105dcb32fa4029ca9770f264f4072045b0c5932` 执行唯一 registered source
  acquisition。179 个 Federal Reserve/BLS 请求全部 HTTP 200，保存
  `179 files / 160417518 bytes`；首次 source manifest、attempt ledger 与 blocked gate
  SHA-256 分别为
  `4d7cc17dc8d4347d57b7df7912309ef60a1dfe62c3247b330ce24f7c18571575`、
  `8656a70679c7d8f87a95efa707c1973fec4adbc6fa8d9cab493a01788b934a09`、
  `fb13674801feebbb234846c2499159da604e85680e0a2dff07a3e3b9f04e6003`。
  初始 gate 因 64 个 `O1_EVENT_BLS_TIMESTAMP_MISSING` 关闭 coverage/model training；
  随后只读审计确认所有 blocker 都来自 NFP 页面 header 中合法的 `USDL` identifier
  插入格式，是 parser defect。下一步先修复 parser 与 immutable raw replay path；在
  superseding replay gate PASS 前不得把初始机械 class 当作研究结论，也不得进入 coverage。
- 2026-07-30：完成 bounded `USDL` header parser correction 与 checksum-verified
  offline replay path。修复要求 BLS 正文日期/weekday 与 archive URL 日期一致；replay
  只接受 initial manifest/attempt/gate 三个显式 SHA，逐项复验 179 份 retained official
  bytes、index→release inventory、policy/DQ/attempt identity，且 API 没有 fetcher。
  实际 retained bytes 的只读预演为 `179 requests / 179 artifacts / 171 events`
  （FOMC 43 / CPI 64 / NFP 64），未创建 replay 输出。focused=`11/11`、
  authority/deprecation=`152/152`、Architecture=`793/793`
  （`outputs/validation_runtime/architecture-fitness_20260729T200434Z/`）、
  Contract=`276/276`
  （`outputs/validation_runtime/contract-validation_20260729T200622Z/`）。
  下一步只允许发布该 exact code commit，再从该 commit 在同一 root 执行一次 offline
  replay；发布前不得生成 superseding gate。
- 2026-07-30：parser/replay commit
  `9dc8f48b465bdf973ffe926c92a3a4257c716028` 已 fast-forward 到 local main 并
  ordinary push，确认 local main 与 origin/main 相同。随后在同一 registered root
  执行唯一正式 offline replay；首次三份 failure artifacts SHA 全部不变，新 source
  manifest/event ledger/gate SHA 分别为
  `59f72415413afb0fcb12e7f3052f717c0aaec19fc0eaa059c5b5c1248ee59f1f`、
  `d714af0779e6edb97f9ed143192c7b8858e70f21196e96be190b837cc0deb476`、
  `5855720e4f0d5f97748730d5f90cbba18d852c5c90c67197229832c3d51faba5`。
  gate=`PASS_EVENT_AND_ATTEMPT_LEDGERS_FROZEN`，171 个 event 逐行回链 retained raw
  checksum；只授权 coverage-only，model training/canonical run/production 仍关闭。
  active policy 已串行绑定 exact evidence；下一步从该合同集成 commit 实现 coverage-only
  runner，不得在同一 wave 读取 eligibility count。
- 2026-07-30：coverage-only runner 已在 controlled/synthetic fixtures 上实现并完成首轮
  parallel focused `11/11 PASS`。runner 在任何真实 eligibility 读取前逐 SHA 复验 policy、
  DQ/event/attempt evidence 与三份 candidate input；随后只输出 fold/ESS、train-only
  regime tertile 和 event-episode coverage，不保存逐行 target/features，不训练模型、不生成
  prediction/metric。active policy 推进到 coverage-ready 后，历史 synthetic builder tests
  曾因继续读取 active policy 而出现 5 项回归；修复是测试专用 pre-coverage policy copy，
  production synthetic validator 的 `real_coverage_read_allowed_now=false` fail-closed 检查
  保持不变，回归恢复 `5/5 PASS`。当前仍未读取真实 coverage；下一步必须先完成
  runner/formal validation、发布 exact commit，再在 8.3 唯一目录执行一次。
- 2026-07-30：coverage-only runner 发布前门禁完成。O1 五文件并行 focused=`38/38`，
  authority/deprecation=`154/154`，Ruff targeted=`PASS`，Architecture=`795/795`
  （`outputs/validation_runtime/architecture-fitness_20260729T211248Z/`），
  Contract=`276/276`
  （`outputs/validation_runtime/contract-validation_20260729T211446Z/`）。deprecation
  inventory 只因新增一个 module 与一个 test file，从 `1042/1211` 机械更新为
  `1043/1212`，所有 removal gate 仍关闭。真实 coverage 继续为未执行；下一步只允许
  commit、fast-forward local `main`、ordinary push，然后从该 published exact commit 在
  8.3 唯一目录运行一次。
- 2026-07-30：runner commit
  `1bf9fb13245064ec2a505ea864e2e127ad445d41` 已 fast-forward 到 local `main` 并 ordinary
  push，确认 local main 与 origin/main 相同。随后只在 8.3 唯一目录执行一次真实
  coverage-only run；DQ 继续为 `PASS/0/0`，6 个 completed outer folds、total OOF effective
  sample=`146 >= 120`，FOMC/CPI/NFP event coverage 均 PASS，但四个 mandatory floor
  失败：F01 train=`98 < 100`、F02 test ESS=`23.71930136737 < 24`、volatility HIGH
  fold count=`2 < 3`、current_drawdown LOW effective sample=`13 < 15`。因此 gate
  `o1_coverage_gate_b240158b3b7d3211ad51852217aa6d93` 机械输出
  `INSUFFICIENT_COVERAGE_OR_DQ`。没有调整 threshold、fold、horizon、regime 或 event，
  没有第二次 run；canonical policy update、model training、canonical run 与 production
  全部保持 false，当前 O1 capability path 关闭。
- 2026-07-30：coverage report/gate exact SHA 已进入独立 serial evidence-binding
  closeout；active policy 状态=`CLOSED_INSUFFICIENT_COVERAGE_OR_DQ`，single-run
  permission 已消耗。O1 五文件回归=`38/38`、authority/deprecation=`155/155`、
  Architecture=`796/796`
  （`outputs/validation_runtime/architecture-fitness_20260729T214458Z/`）、
  Contract=`276/276`
  （`outputs/validation_runtime/contract-validation_20260729T214645Z/`）。任务转为
  `BASELINE_DONE`；若未来重开必须另立新任务和结果读取前 preregistration，不得从本结果
  反向调整本 attempt 的设计。
