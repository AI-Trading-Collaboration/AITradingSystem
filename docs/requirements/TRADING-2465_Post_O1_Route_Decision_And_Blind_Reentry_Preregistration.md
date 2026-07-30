# TRADING-2465：Post-O1 Route Decision And Blind Re-entry Preregistration

最后更新：2026-07-30

稳定任务 ID：
`TRADING-2465_POST_O1_ROUTE_DECISION_AND_BLIND_REENTRY_PREREGISTRATION`

优先级：`P0`

状态：`BLOCKED_OWNER_INPUT`

production effect：`none`

broker action：`none`

## 1. Owner 指令与授权解释

2026-07-30，Owner 要求：

```text
根据回答内容来推进后续任务
```

该自然语言指令授权建立 post-O1 路线决策包、inactive preregistration proposal、prior-attempt
与 multiplicity 合同，以及证明 no-result-read/downstream-disabled 的验证基础设施。它不被
扩大解释为：

- 已选择某个 post-O1 route；
- 已批准 `2027-02-01` 或任何其他 calendar trigger；
- 已批准未来 12 个月一次 look 或其他 numeric look budget；
- 已授权读取新的 O1/O2/O3 count、coverage、event base rate、target、prediction 或 metric；
- 已授权重新执行 coverage、训练模型、canonical run、falsification、Decision Value Audit、
  backtest、weights、paper-shadow、production 或 broker action。

因此本任务完成 planning/preregistration 基础设施后，必须停在
`BLOCKED_OWNER_INPUT`，直到 Owner 给出第 8 节的 exact decision。

## 2. 当前不可变事实

权威 Git snapshot：

```text
repository=https://github.com/AI-Trading-Collaboration/AITradingSystem
exact_commit=fb4687244e04228ae2e5c4dd425f82cb1e35291c
```

TRADING-2464 的唯一 coverage-only attempt 已消耗并机械关闭：

- task status=`BASELINE_DONE`；
- policy status=`CLOSED_INSUFFICIENT_COVERAGE_OR_DQ`；
- actual cause=`mandatory coverage insufficiency`；
- capability status=`NOT_EVALUATED`，不是 `NO_MEASURABLE_SKILL`；
- DQ=`PASS/0/0`；
- completed outer folds=`6`；
- total OOF effective sample=`146 >= 120`；
- FOMC/CPI/NFP event coverage=`PASS`；
- mandatory failures：
  - F01 train effective sample=`98 < 100`；
  - F02 test ESS=`23.71930136737 < 24`；
  - volatility HIGH fold count=`2 < 3`；
  - current_drawdown LOW effective sample=`13 < 15`。

永久 prior-attempt evidence：

```text
attempt_id=O1_M1_RIDGE_CROSS_ASSET_STATE_V1
coverage_source_commit=1bf9fb13245064ec2a505ea864e2e127ad445d41
coverage_report_id=o1_coverage_report_9b5708c6c36ac69cc7355fee8567a953
coverage_report_sha256=bbed79b499b57274dd49bede0c37219894233964732fcde5656626933781ada7
coverage_gate_id=o1_coverage_gate_b240158b3b7d3211ad51852217aa6d93
coverage_gate_sha256=a97ee44832a41aeb90a6f9a18b0358eb81cefec4d491438deb6fd27b624f31b8
single_run_consumed=true
model_training_executed=false
prediction_or_metric_generated=false
prospective_accessed=false
```

本任务不得打开、复制、覆盖、删除或重新解释 retained runtime evidence root。Git 中的
SHA/摘要绑定不能替代原始 bytes；原始 bytes 继续受 TRADING-2464 生命周期规则保护。

## 3. Web Pro advisory evidence 与本地对账

审阅对话：
`https://chatgpt.com/c/6a6aa100-347c-83ee-9af9-4c1709761389`

审阅 exact commit：
`fb4687244e04228ae2e5c4dd425f82cb1e35291c`

Git retrieval：

- `AGENTS.md`：`SUCCESS`；
- `docs/task_register.md`：`PARTIAL`，但 exact 定位 TRADING-2464 行；
- TRADING-2464 requirement：`SUCCESS`；
- active O1 policy：`SUCCESS`；
- TRADING-2463 S3 pack：`SUCCESS`；
- TRADING-2463 S1/S2 pack：`SUCCESS`；
- failed blob retrieval：`0`；
- 网页报告的六个 blob SHA 与本地 exact Git blob SHA 一致。

路由风险：

```text
UI_SELECTION=account_Pro_and_composer_Pro
MODEL_SELF_REPORT=GPT-5.6_Pro
BACKEND_ROUTE_ATTESTATION=UNAVAILABLE
classification=UI_PRO_AND_SELF_REPORT_PRO_ROUTE_UNVERIFIED
secondary_classification=ROUTING_ATTESTATION_UNAVAILABLE
CANNOT_VERIFY_EXACT_BACKEND_ROUTE=true
```

网页回答是 advisory planning evidence，不是后端路由证明、仓库 policy 或 Owner
authorization。本地采纳其与既有规则一致的部分：

1. 主推荐 `A + D`：保持 V1 关闭，先冻结 blind calendar re-entry，再只做 generic
   data/evidence infrastructure；
2. O2 只能作为独立 `PATH_LOSS_BUDGET_EVENT` policy foundation，不得作为 O1 rescue；
3. O3 utility/action/cost policy 自由度过高，近期 defer；
4. O4 不得在 O1/O2 未分别证明 capability 前启动；
5. 完全关闭 redesign 是合法 off-ramp；
6. 第一合法动作是 planning/preregistration，而不是 empirical run。

以下网页建议不直接采纳为 active policy：

- `candidate_not_before_date=2027-02-01`；
- `maximum_reentry_coverage_looks_in_next_12_months=1`；
- 网页提出但仓库尚未 reviewed 的新 typed status 名称；
- 任何 0–1、1–3、2–6 或 6–12 个月的进度估计。

这些只能进入 inactive Owner decision slot，并必须在读取新 count 前由 Owner 给出 rationale
和 exact token。

## 4. 推荐路线与 Owner 选择位

### Route A + D（推荐但未批准）

```text
A_PLUS_D_O1_BLIND_CALENDAR_REENTRY_WITH_GENERIC_EVIDENCE_INFRASTRUCTURE
```

- V1 永久保持 consumed/closed；
- 在任何新 eligibility look 前冻结 exact calendar trigger、new data vintage、V2 attempt
  identity、maximum look budget 和 stopping rule；
- trigger 前只允许正常 cadence 的 generic source acquisition、DQ/PIT、publication receipt、
  checksum、evidence retention 与 recovery rehearsal；
- 不得计算新的 O1 fold/ESS/regime/event coverage；
- trigger 到期不表示预期 PASS，只表示可由新的 Owner run authorization 再决定是否运行；
- re-entry 只能使用新 task、新 attempt、新 data vintage，并 exact 复用 V1 的 target/form/
  horizon/split/floors/regime/event/model/metric/falsification。

### Route B（独立 policy foundation）

```text
O2_PATH_LOSS_BUDGET_EVENT_POLICY_FOUNDATION_ONLY
```

- 只冻结 loss budget、continuous/binary form、event base-rate policy、false-negative cost、
  calibration gate 与独立 event ledger；
- 不读取 O2 event count 或 target result；
- 不继承 O1、旧 worst-1d 或 tail-risk positive evidence；
- 独立进入 global redesign multiplicity；
- foundation PASS 也只允许另立 O2 capability task，不自动成为 risk overlay。

### Route C（defer）

```text
O3_UTILITY_ACTION_COST_POLICY_FOUNDATION
```

只有 Owner 能在不查看 historical winner 的情况下冻结 action templates、execution timing、
cost model、risk penalty、utility unit 与 sensitivity policy 时才可另立任务。当前不推荐。

### Route E（clean off-ramp）

```text
CLOSE_POST_O1_DECISION_TARGET_REDESIGN
```

关闭 O1/O2/O3/O4 当前 redesign，保留 evidence。未来重启必须使用新任务、新 preregistration，
不得静默改名继续。

## 5. 本任务范围与明确禁止

本任务只负责：

- task register 与 supporting requirement；
- Owner route decision pack；
- inactive proposal；
- prior-attempt/multiplicity contract；
- calendar trigger、data vintage、look budget、stopping rule 的未选择 slot；
- evidence retention/no-result-read contract；
- validator 与 controlled/static tests；
- generated task/architecture governance state；
- formal validation 与 Owner handoff。

本任务不得：

- 读取 retained runtime evidence bytes 或任何新的 eligibility/count；
- 运行 data acquisition、DQ、coverage、target、feature、model、prediction、metric 或
  falsification；
- 修改 O1 target、continuous form、5 common sessions horizon、fold、purge、embargo、
  coverage floors、regime/event definition、model family、metric 或 falsification；
- 把 2021-02-22 以前的数据补入 primary window；
- 把 O2/O3/O4、QLD、TQQQ 或历史 tail-risk evidence用作 O1 rescue；
- 创建 Decision Value Audit、risk overlay、candidate/backtest/weights、paper-shadow、
  promotion、production 或 broker action；
- 修改或清理 TRADING-2464 retained evidence root。

本任务不改变实际数据流，因此不要求修改 `docs/system_flow.md`。如果实现范围后来新增 CLI、
runtime、cache/report schema 或数据流，必须先更新本 requirement、重新声明 claims 并在同一
change 更新 system flow。

## 6. 分步计划与依赖

### S0：任务登记

- 建立本任务行与本 supporting requirement；
- status=`IN_PROGRESS`；
- 未建立 active policy。

### S1：Inactive proposal 与 prior-attempt freeze

- exact 绑定 TRADING-2464 report/gate/attempt；
- 复制的是既有 policy identity/value，不读取 runtime result；
- route、calendar date、data vintage、look budget 保持 `NOT_SELECTED`；
- 所有 empirical/downstream authorization=false。

### S2：Independent validator 与 tests

- validator 从 proposal 重建 required identities 和安全状态；
- 拒绝缺失 V1、篡改 SHA、激活 route、填写未授权日期/look budget、允许新 result read、
  修改 O1 frozen values、允许 model/canonical/downstream；
- deterministic serialization 与 schema validation通过。

### S3：Generated governance 与 formal validation

- 刷新 task shadow/architecture manifests；
- focused/authority/Architecture/Contract validation通过；
- status 转为 `BLOCKED_OWNER_INPUT`；
- 输出 Owner decision token，不运行任何 empirical action。

### S4：Owner decision 后的未来任务

本任务不自动执行 S4。Owner 决定必须进入新的 serial contract wave；若选择 A+D，具体 re-entry
run 仍需另一个显式 Owner authorization。

## 7. Acceptance criteria

1. task row、supporting requirement 与 inactive proposal存在且互相引用；
2. V1 consumed coverage look、report/gate ID/SHA、source commit exact 绑定；
3. V1 的 target/form/horizon/split/floors/regime/event/model/metric/falsification无变化；
4. route、calendar date、data vintage 与 look budget在 Owner 决定前为 `NOT_SELECTED`；
5. proposal 不能被 production/runtime loader当作 active policy；
6. validator 对 missing/tamper/unauthorized activation fail closed；
7. `new_result_read_allowed=false`、`coverage_run_allowed=false`、
   `model_training_allowed=false`、`canonical_run_allowed=false`；
8. Decision Value Audit、risk overlay、candidate/backtest/weights、paper-shadow、production、
   broker action全部为 false/none；
9. 未读取或修改 TRADING-2464 retained runtime evidence；
10. no-result-read attestation、focused tests、generated freshness、Architecture 与 Contract
    validation通过；
11. 当前 worktree/main/remote按项目默认流程安全收口；
12. supporting requirement 与 task row 状态同步。

## 8. Owner decision tokens

推荐 Route A + D：

```text
owner_decision:TRADING-2465:YYYY-MM-DD:select_o1_blind_calendar_reentry_with_generic_evidence_infrastructure_v1
```

该 token 仍不完整授权 empirical action。Owner 必须同时批准 exact calendar trigger、
maximum look budget 与 data-vintage rule，或授权另行形成完整 serial policy proposal。

独立 Route B：

```text
owner_decision:TRADING-2465:YYYY-MM-DD:authorize_independent_o2_path_loss_budget_policy_foundation_v1
```

Route C：

```text
owner_decision:TRADING-2465:YYYY-MM-DD:authorize_o3_utility_action_cost_policy_foundation_v1
```

Route E：

```text
owner_decision:TRADING-2465:YYYY-MM-DD:close_post_o1_decision_target_redesign_v1
```

Owner 也可选择 A+D，并以第二个独立 token 额外授权 B foundation；这两条路线的 task、
policy、evidence lineage 与 multiplicity 必须分离。

## 9. Stop matrix

|阶段|PASS|FAIL|INSUFFICIENT|INVALID|
|---|---|---|---|---|
|本 planning/preregistration|Owner route、exact trigger policy slots、V1 accounting与downstream boundary完整；只允许进入未来 serial contract wave|Owner拒绝或选择 Route E，关闭当前路线|policy/rationale/data-vintage/evidence-retention设计不足，保持`BLOCKED_OWNER_INPUT`|基于已知四个 failed cells修改 threshold/fold/horizon/regime/event/model，标记`INVALID_POST_RESULT_REDESIGN_CONTAMINATION`并停止|
|未来 O1 coverage re-entry|全部 mandatory coverage floor PASS，只允许 evidence binding 与新 Owner canonical gate|不适用；coverage gate 不产生`NO_MEASURABLE_SKILL`|任一 mandatory floor不足，输出`INSUFFICIENT_COVERAGE_OR_DQ`并关闭该 attempt|leakage、attempt contamination、lineage tamper、exact reconstruction mismatch或validator error，不产生 capability class|
|O2 foundation|policy slots经 Owner review，只允许另立 capability task|无法形成稳定、独立经济 rationale，关闭 foundation|source/base-rate feasibility/DQ/calendar/cost evidence不足，不生成 target|把 O2 当 O1 rescue、继承旧 evidence或在看 count 后选 budget/form，停止且不产生资格|
|O3 foundation|action/utility/cost slots全部先验冻结，只允许另立 capability task|合理 policy variations 下 action winner不稳定，关闭 foundation|cost/execution/risk/action maturity不足，不生成 net-utility label|同一历史结果同时选择 action、utility、risk penalty、cost与model，停止|

仓库既有 capability class 仍只有 TRADING-2464 定义的四类；本任务中新增的 planning/foundation
文字只作为 proposal status，除非后续 reviewed contract 正式定义，否则不得冒充当前
machine-readable class。

## 10. First stop condition

```text
IF owner_route_token is absent
OR route-specific policy slots remain NOT_SELECTED
OR prior_attempt_accounting is incomplete
THEN status=BLOCKED_OWNER_INPUT
AND no empirical action is allowed
```

若出现任何针对已知 `98/100`、`23.71930136737/24`、volatility HIGH `2/3` 或
current_drawdown LOW `13/15` 的 result-driven redesign，则立即停止，不得通过更换 task ID、
target 名称或 workspace 继续。

## 11. 临时工作区与证据生命周期

本 planning wave 不创建额外 Git worktree、clone、candidate、cache、runtime output 或
empirical artifact。唯一任务分支在完成验证、main fast-forward、remote push与恢复性审计后
删除；实现由 Git main history 恢复。

TRADING-2464 retained evidence root保持原位且禁止读取/修改/清理。本任务只引用其已进入 Git
authority 的 ID/SHA。

## 12. 进度记录

- 2026-07-30：Owner要求根据 Web Pro 回答推进。创建任务与 supporting requirement；
  当前只授权 planning/preregistration infrastructure，不授权 route、日期、look budget或
  empirical action。
- 2026-07-30：inactive proposal 与 independent validator/tests 已实现；proposal exact
  绑定 TRADING-2464 V1 report/gate、closed policy 文件 SHA、Git blob SHA 与九个冻结
  contract section hash。route、日期、data vintage、look budget均保持`NOT_SELECTED`；
  task进入`BLOCKED_OWNER_INPUT`并等待验证与Owner选择。
- 2026-07-30：首次 validator/focused 检查暴露 requirement 中文状态标签解析错误
  （17 passed / 1 failed），已直接修复且没有降低门禁；最终 validator=`PASS_0_ERRORS`，
  focused=`27 passed`，task registry=`PASS_BYTE_IDENTICAL_928_TASKS`，
  generated architecture=`PASS_1043_MODULES_1213_TESTS`，
  authority/deprecation=`156 passed`。
- 2026-07-30：首次 Architecture 在新 append-only authority 未接管生成清单漂移时
  fail closed（56 failed / 740 passed）；补充 TRADING-2465 EOF current-hash authority
  后第二轮仅剩 6 个历史阶段的后续接管声明失败，均直接修正；最终 Architecture
  `797 passed`，artifact=`outputs/validation_runtime/architecture-fitness_20260730T014012Z/test_runtime_summary.json`。
  Contract 最终 `276 passed`，artifact=`outputs/validation_runtime/contract-validation_20260730T014251Z/test_runtime_summary.json`。
  route、calendar、data vintage、look budget与所有 empirical/downstream action仍未授权。
