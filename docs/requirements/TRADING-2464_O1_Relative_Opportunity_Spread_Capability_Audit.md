# TRADING-2464：O1 Relative Opportunity Spread Capability Audit

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2464_O1_RELATIVE_OPPORTUNITY_SPREAD_CAPABILITY_AUDIT`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

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
`BLOCKED_OWNER_INPUT`。

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

## 8. 当前停止点

- `status=BLOCKED_OWNER_INPUT`
- `data_prerequisite=DOCUMENTED_D0B2B_STRICT_PASS_BUT_RECEIPT_BYTES_MISSING`
- `owner_authorization_for_capability_audit=false`
- `model_feature_family_authority=UNRESOLVED_BEFORE_RESULT_READ`
- `model_feature_family_proposal=OWNER_REVIEW_REQUIRED_NOT_ACTIVE`
- `required_dq_receipt_bytes_present=false`
- `dq_exact_chain_recoverable_from_ops_runtime=true`
- `dq_isolated_candidate_materialized=false`
- `new_results_read=false`
- `prospective_accessed=false`
- `model_training_executed=false`
- `decision_value_audit_started=false`
- `risk_overlay_created=false`
- `candidate_backtest_weights_created=false`
- `qld_automatic_selection_enabled=false`
- `production_effect=none`
- `broker_action=none`

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
